package engine

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"comet/internal/logger"
	"comet/internal/models"
	"comet/internal/network"
	"comet/internal/storage"
)

type Sender struct {
	transport    network.Transport
	store        storage.Store
	chunkSize    int64
	maxWorkers   int
	timeout      time.Duration
	token        string
	progressFunc func(sessionID string, transferred int64, total int64)
	logger       *logger.Logger
}

func NewSender(transport network.Transport, store storage.Store, chunkSize int64, maxWorkers int, timeout time.Duration, token string, log *logger.Logger) *Sender {
	return &Sender{
		transport:  transport,
		store:      store,
		chunkSize:  chunkSize,
		maxWorkers: maxWorkers,
		timeout:    timeout,
		token:      token,
		logger:     log,
	}
}

func (s *Sender) SetProgressCallback(fn func(sessionID string, transferred int64, total int64)) {
	s.progressFunc = fn
}

func (s *Sender) SendFile(ctx context.Context, filePath string, targetAddr string) error {
	return s.SendStream(ctx, filePath, targetAddr, false)
}

func (s *Sender) SendFolder(ctx context.Context, folderPath string, targetAddr string) error {
	return s.SendStream(ctx, folderPath, targetAddr, true)
}

func (s *Sender) SendStream(ctx context.Context, path string, targetAddr string, isFolder bool) error {
	sessionID := uuid.New().String()
	s.logger.Infof("[Sender] 开始传输: %s -> %s, session: %s", path, targetAddr, sessionID)

	conn, err := s.transport.Dial(ctx, targetAddr)
	if err != nil {
		return fmt.Errorf("连接失败: %w", err)
	}
	defer conn.Close()

	if err := s.authenticate(conn); err != nil {
		return err
	}

	var totalSize int64
	var displayName string
	var finalPath string
	var isTempFile bool
	if isFolder {
		// 1. 创建临时文件
		tmpFile, err := os.CreateTemp("", "comet_zip_*.zip")
		if err != nil {
			return err
		}
		// 注意：先不 defer close，后面复用完再删

		// 2. 压缩到临时文件
		if err := s.zipFolder(path, tmpFile); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return err
		}

		// 3. 获取准确大小，并重置文件指针到开头
		info, _ := tmpFile.Stat()
		totalSize = info.Size()
		displayName = filepath.Base(path) + ".zip"

		// 4. 关闭句柄，让后续的 os.Open 重新打开（避免指针状态混乱）
		tmpFile.Close()

		finalPath = tmpFile.Name()
		isTempFile = true // 标记传输完成后要删除
		isFolder = false  // 关键：欺骗后续逻辑，把它当普通文件传输

		s.logger.Infof("[Sender] 文件夹已打包为临时文件: %s (大小: %d)", finalPath, totalSize)
	} else {
		// 普通文件逻辑
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		info, _ := file.Stat()
		totalSize = info.Size()
		displayName = filepath.Base(path)
		finalPath = path
	}
	// 4. 发送元数据
	meta := fmt.Sprintf("%s|%d|%s", displayName, totalSize, sessionID)
	if err := conn.SendPacket(network.CmdMeta, []byte(meta)); err != nil {
		return err
	}

	// 5. 等待元数据确认
	cmd, payload, err := conn.ReadPacket()
	if err != nil {
		return err
	}
	if cmd == network.CmdError {
		return fmt.Errorf("接收端错误: %s", string(payload))
	}
	if cmd != network.CmdMetaAck {
		return fmt.Errorf("意外的响应: 0x%02X", cmd)
	}

	// 6. 检查断点续传
	var completed []bool
	if err := s.loadCheckpoint(sessionID, &completed); err == nil && len(completed) > 0 {
		s.logger.Infof("[Sender] 发现断点记录, 已完成 %d/%d 块", countTrue(completed), len(completed))
		// 询问对端已接收情况
		if err := conn.SendPacket(network.CmdQuery, []byte(sessionID)); err != nil {
			return err
		}
		cmd, _, err := conn.ReadPacket()
		if err == nil && cmd == network.CmdQueryResp {
			// 合并进度
			// 简化实现：仅使用本地记录
		}
	}

	// 7. 分块传输
	chunkSize := s.chunkSize
	totalChunks := int((totalSize + chunkSize - 1) / chunkSize)
	completed = make([]bool, totalChunks)
	s.logger.Infof("[Receiver] 总块: %d 总大小: %d ", totalChunks, totalSize)
	var transferred int64
	sharedFile, err := os.Open(finalPath)
	if err != nil {
		return err
	}
	defer sharedFile.Close()

	// 使用 errgroup 并发上传
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(s.maxWorkers)

	var chunkIdx int
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if chunkIdx >= totalChunks {
			break
		}

		// 跳过已完成的块（断点续传）
		if completed[chunkIdx] {
			chunkIdx++
			continue
		}

		idx := chunkIdx
		chunkIdx++

		g.Go(func() error {
			offset := int64(idx) * chunkSize
			size := chunkSize
			if offset+size > totalSize {
				size = totalSize - offset
			}

			// 读取分块数据（使用共享文件句柄，ReadAt 线程安全）
			data := make([]byte, size)
			_, err := sharedFile.ReadAt(data, offset)
			if err != nil && err != io.EOF {
				return err
			}

			// 构建分块报文：前8字节存放块索引和长度（简单用字符串拼接）
			header := make([]byte, 8)
			binary.BigEndian.PutUint32(header[0:4], uint32(idx))
			binary.BigEndian.PutUint32(header[4:8], uint32(len(data)))

			chunkPayload := make([]byte, 8+len(data))
			copy(chunkPayload[0:8], header)
			copy(chunkPayload[8:], data)

			// 发送分块
			if err := conn.SendPacket(network.CmdChunk, chunkPayload); err != nil {
				return err
			}

			// 更新进度
			atomic.AddInt64(&transferred, size)
			completed[idx] = true

			// 保存断点
			s.saveCheckpoint(sessionID, completed)

			if s.progressFunc != nil {
				// 注意：transferred 是原子值，这里直接读取（可能稍有偏差，但可接受）
				s.progressFunc(sessionID, atomic.LoadInt64(&transferred), totalSize)
			}
			s.logger.Debugf("[Sender] 分块 %d 已发送 (%d bytes)", idx, size)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// 8. 发送完成
	if err := conn.SendPacket(network.CmdComplete, []byte(sessionID)); err != nil {
		return err
	}
	if isTempFile {
		os.Remove(finalPath)
		s.logger.Infof("[Sender] 已清理临时压缩包: %s", finalPath)
	}
	s.store.DeleteCheckpoint(sessionID)
	s.logger.Infof("[Sender] ✅ 传输完成: %s -> %s", displayName, targetAddr)
	return nil
}

func (s *Sender) authenticate(conn network.Conn) error {
	if err := conn.SendPacket(network.CmdAuth, []byte(s.token)); err != nil {
		return err
	}
	cmd, payload, err := conn.ReadPacket()
	if err != nil {
		return err
	}
	if cmd == network.CmdAuthFail {
		return fmt.Errorf("认证失败: %s", string(payload))
	}
	if cmd != network.CmdAuthOK {
		return fmt.Errorf("认证响应异常: 0x%02X", cmd)
	}
	s.logger.Debug("[Sender] 认证成功")
	return nil
}

func (s *Sender) zipFolder(folderPath string, w io.Writer) error {
	zw := zip.NewWriter(w)
	defer zw.Close()

	return filepath.WalkDir(folderPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(folderPath, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)

		fw, err := zw.Create(relPath)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(fw, file)
		return err
	})
}

func (s *Sender) folderSize(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func (s *Sender) saveCheckpoint(sessionID string, completed []bool) {
	cp := &models.Checkpoint{
		SessionID:   sessionID,
		TotalChunks: len(completed),
		ChunkSize:   s.chunkSize,
		Completed:   completed,
		UpdatedAt:   time.Now(),
	}
	s.store.SaveCheckpoint(sessionID, cp)
}

func (s *Sender) loadCheckpoint(sessionID string, completed *[]bool) error {
	cp, err := s.store.LoadCheckpoint(sessionID)
	if err != nil {
		return err
	}
	*completed = cp.Completed
	return nil
}

func countTrue(b []bool) int {
	count := 0
	for _, v := range b {
		if v {
			count++
		}
	}
	return count
}
