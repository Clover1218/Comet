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
	"comet/internal/zipper"
)

type Sender struct {
	transport    network.Transport
	store        storage.Store
	chunkSize    int64
	maxWorkers   int
	timeout      time.Duration
	token        string
	progressFunc func(sessionID string, transferred int64, total int64)

	logger *logger.Logger
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
	return s.SendStream(ctx, filePath, targetAddr, false, "")
}

func (s *Sender) SendFolder(ctx context.Context, folderPath string, targetAddr string) error {
	return s.SendStream(ctx, folderPath, targetAddr, true, "")
}
func (s *Sender) SendFileBySession(ctx context.Context, filePath string, targetAddr string, sessionID string) error {
	return s.SendStream(ctx, filePath, targetAddr, false, sessionID)
}

func (s *Sender) SendFolderBySession(ctx context.Context, folderPath string, targetAddr string, sessionID string) error {
	return s.SendStream(ctx, folderPath, targetAddr, true, sessionID)
}

func (s *Sender) SendStream(ctx context.Context, path string, targetAddr string, isFolder bool, sessionID string) error {
	if sessionID == "" {
		sessionID = uuid.New().String()
		var completed []bool
		s.createCheckpoint(sessionID, completed, path, isFolder, targetAddr)

	}

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
		// 临时压缩包路径（基于 sessionID，断点续传时可复用）
		ext := zipper.NewZstdZipper(0).Extension()
		tmpFilePath := filepath.Join("./data/tmp", sessionID+"."+ext)

		// 如果文件已存在则跳过重新压缩
		if _, err := os.Stat(tmpFilePath); os.IsNotExist(err) {
			if err := zipper.NewZstdZipper(0).Pack(path, tmpFilePath); err != nil {
				os.Remove(tmpFilePath)
				return fmt.Errorf("压缩失败: %w", err)
			}
			s.logger.Infof("[Sender] 文件夹已打包为临时文件: %s", tmpFilePath)
		} else {
			s.logger.Infof("[Sender] 复用已有临时压缩包: %s", tmpFilePath)
		}

		info, err := os.Stat(tmpFilePath)
		if err != nil {
			return fmt.Errorf("打开压缩包失败: %w", err)
		}
		totalSize = info.Size()
		displayName = filepath.Base(path) + "." + ext
		finalPath = tmpFilePath
		isTempFile = true

		s.logger.Infof("[Sender] 总大小: %d", totalSize)
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

	// 确保传输完毕后删除临时压缩包（如果有）
	defer func() {
		if isTempFile {
			os.Remove(finalPath)
			s.logger.Infof("[Sender] 已清理临时压缩包: %s", finalPath)
		}
	}()

	isFolderFlag := 0
	if isFolder {
		isFolderFlag = 1
	}
	meta := fmt.Sprintf("%s|%d|%s|%d", displayName, totalSize, sessionID, isFolderFlag)
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

	// 6. 断点续传 - 交换双方 completed 位图，取并集
	chunkSize := s.chunkSize
	totalChunks := int((totalSize + chunkSize - 1) / chunkSize)

	var localCompleted []bool
	cp, err := s.store.LoadCheckpoint(sessionID)
	if err == nil && cp != nil {
		localCompleted = cp.Completed
		s.logger.Infof("[Sender] 发现本地断点记录, 已完成 %d/%d 块", countTrue(localCompleted), totalChunks)
	}

	// 向接收端查询已完成块
	if err := conn.SendPacket(network.CmdQuery, []byte(sessionID)); err != nil {
		return err
	}
	cmd, payload, err = conn.ReadPacket()
	if err != nil {
		return err
	}

	var remoteCompleted []bool
	if cmd == network.CmdQueryResp {
		remoteCompleted = bytesToBools(payload)
		s.logger.Infof("[Sender] 接收端已完成 %d 块", countTrue(remoteCompleted))
	}

	// 合并双方进度（取并集）
	completed := mergeCompleted(localCompleted, remoteCompleted)

	// 补齐到 totalChunks 长度（首次传输时双方都可能为空）
	if len(completed) < totalChunks {
		tmp := make([]bool, totalChunks)
		copy(tmp, completed)
		completed = tmp
	}

	// 将合并后的位图同步给接收端，并保存本地
	if err := conn.SendPacket(network.CmdChkSync, boolsToBytes(completed)); err != nil {
		return err
	}
	s.store.UpdateCheckpoint(sessionID, func(cp *models.Checkpoint) {
		c := make([]bool, len(completed))
		copy(c, completed)
		cp.Completed = c
	})
	s.logger.Infof("[Sender] 断点合并后: 已完成 %d/%d 块", countTrue(completed), totalChunks)

	// 7. 分块传输
	s.logger.Infof("[Sender] 总块: %d 总大小: %d ", totalChunks, totalSize)
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

		if chunkIdx < len(completed) {
			if completed[chunkIdx] == true {
				chunkIdx++

				continue
			}

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
			s.store.UpdateCheckpoint(sessionID, func(cp *models.Checkpoint) {
				c := make([]bool, len(completed))
				copy(c, completed)
				cp.Completed = c
			})

			if s.progressFunc != nil {
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

func (s *Sender) createCheckpoint(sessionID string, completed []bool, filePath string, isFolder bool, addr string) {
	cp := &models.Checkpoint{
		SessionID:   sessionID,
		TotalChunks: len(completed),
		ChunkSize:   s.chunkSize,
		Completed:   completed,
		UpdatedAt:   time.Now(),
		FilePath:    filePath,
		IsFoler:     isFolder,
		TargetAddr:  addr,
	}
	s.store.SaveCheckpoint(sessionID, cp)
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
