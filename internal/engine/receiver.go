package engine

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"comet/internal/logger"
	"comet/internal/models"
	"comet/internal/network"
	"comet/internal/storage"
)

type Receiver struct {
	transport    network.Transport
	store        storage.Store
	saveDir      string
	chunkSize    int64
	timeout      time.Duration
	token        string
	progressFunc func(sessionID string, transferred int64, total int64)
	authFunc     func(token string) bool
	logger       *logger.Logger
}

func NewReceiver(transport network.Transport, store storage.Store, saveDir string, chunkSize int64, timeout time.Duration, token string, log *logger.Logger) *Receiver {
	return &Receiver{
		transport: transport,
		store:     store,
		saveDir:   saveDir,
		chunkSize: chunkSize,
		timeout:   timeout,
		token:     token,
		logger:    log,
	}
}

func (r *Receiver) SetProgressCallback(fn func(sessionID string, transferred int64, total int64)) {
	r.progressFunc = fn
}

func (r *Receiver) SetAuthFunc(fn func(token string) bool) {
	r.authFunc = fn
}

func (r *Receiver) Start(ctx context.Context, listenAddr string) error {
	ln, err := r.transport.Listen(ctx, listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	r.logger.Infof("[Receiver] 监听启动: %s", listenAddr)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			r.logger.Warnf("[Receiver] 接受连接失败: %v", err)
			continue
		}
		go r.handleConnection(ctx, conn)
	}
}

func (r *Receiver) handleConnection(ctx context.Context, conn network.Conn) {
	defer conn.Close()
	r.logger.Infof("[Receiver] 新连接: %s", conn.RawConn().RemoteAddr())

	// 1. 认证
	if err := r.authenticate(conn); err != nil {
		r.logger.Warnf("[Receiver] 认证失败: %v", err)
		return
	}

	// 2. 接收元数据
	cmd, payload, err := conn.ReadPacket()
	if err != nil {
		r.logger.Errorf("[Receiver] 读取元数据失败: %v", err)
		return
	}
	if cmd != network.CmdMeta {
		conn.SendPacket(network.CmdError, []byte("期望META"))
		return
	}

	meta := string(payload)
	parts := strings.SplitN(meta, "|", 3)
	if len(parts) != 3 {
		conn.SendPacket(network.CmdError, []byte("元数据格式错误"))
		return
	}
	fileName := parts[0]
	totalSize, _ := strconv.ParseInt(parts[1], 10, 64)
	sessionID := parts[2]

	r.logger.Infof("[Receiver] 接收: %s, 大小: %d, session: %s", fileName, totalSize, sessionID)

	// 3. 确认元数据
	conn.SendPacket(network.CmdMetaAck, nil)

	// 4. 准备保存路径
	savePath := filepath.Join(r.saveDir, fileName)
	os.MkdirAll(filepath.Dir(savePath), 0755)

	// 检查断点续传
	cp, _ := r.store.LoadCheckpoint(sessionID)
	if cp != nil {
		r.logger.Infof("[Receiver] 断点续传, 已完成 %d/%d 块", countTrue(cp.Completed), cp.TotalChunks)
		// 发送已完成块列表
		// 简化实现
	}

	// 5. 接收分块
	var transferred int64
	totalChunks := int((totalSize + r.chunkSize - 1) / r.chunkSize)
	completed := make([]bool, totalChunks)
	if cp != nil {
		completed = cp.Completed
	}

	// 使用临时文件
	tmpFile := savePath + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		r.logger.Errorf("[Receiver] 创建文件失败: %v", err)
		return
	}
	defer f.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cmd, payload, err := conn.ReadPacket()
		if err != nil {
			r.logger.Errorf("[Receiver] 读取分块失败: %v", err)
			return
		}

		if cmd == network.CmdComplete {
			r.logger.Info("[Receiver] 接收完成")
			break
		}

		if cmd != network.CmdChunk {
			r.logger.Warnf("[Receiver] 未知命令: 0x%02X", cmd)
			continue
		}

		// 解析分块
		chunkParts := strings.SplitN(string(payload[:8]), "|", 2)
		if len(chunkParts) != 2 {
			continue
		}
		idx, _ := strconv.Atoi(chunkParts[0])
		data := payload[8:]

		// 写入文件
		offset := int64(idx) * r.chunkSize
		if _, err := f.WriteAt(data, offset); err != nil {
			r.logger.Errorf("[Receiver] 写入失败: %v", err)
			continue
		}

		completed[idx] = true
		atomic.AddInt64(&transferred, int64(len(data)))

		// 保存进度
		if atomic.LoadInt64(&transferred)%(r.chunkSize*10) == 0 {
			r.saveCheckpoint(sessionID, completed)
		}

		if r.progressFunc != nil {
			r.progressFunc(sessionID, transferred, totalSize)
		}
		r.logger.Debugf("[Receiver] 分块 %d 已接收", idx)
	}

	// 6. 保存最终进度并重命名
	r.saveCheckpoint(sessionID, completed)
	f.Close()

	// 如果是ZIP文件，解压
	if strings.HasSuffix(savePath, ".zip") {
		destDir := strings.TrimSuffix(savePath, ".zip")
		r.logger.Infof("[Receiver] 解压到: %s", destDir)
		if err := unzip(tmpFile, destDir); err != nil {
			r.logger.Errorf("[Receiver] 解压失败: %v", err)
			return
		}
		os.Remove(tmpFile)
	} else {
		os.Rename(tmpFile, savePath)
	}

	// 7. 清理
	r.store.DeleteCheckpoint(sessionID)
	r.logger.Infof("[Receiver] ✅ 接收完成: %s", savePath)
}

func (r *Receiver) authenticate(conn network.Conn) error {
	cmd, payload, err := conn.ReadPacket()
	if err != nil {
		return err
	}
	if cmd != network.CmdAuth {
		conn.SendPacket(network.CmdAuthFail, []byte("期望AUTH"))
		return fmt.Errorf("期望AUTH命令")
	}

	token := string(payload)
	valid := false
	if r.authFunc != nil {
		valid = r.authFunc(token)
	} else {
		valid = token == r.token
	}

	if valid {
		conn.SendPacket(network.CmdAuthOK, nil)
		r.logger.Debug("[Receiver] 认证成功")
		return nil
	}

	conn.SendPacket(network.CmdAuthFail, []byte("无效Token"))
	return fmt.Errorf("认证失败")
}

func (r *Receiver) saveCheckpoint(sessionID string, completed []bool) {
	cp := &models.Checkpoint{
		SessionID:   sessionID,
		TotalChunks: len(completed),
		ChunkSize:   r.chunkSize,
		Completed:   completed,
		UpdatedAt:   time.Now(),
	}
	r.store.SaveCheckpoint(sessionID, cp)
}

func unzip(zipPath, destDir string) error {
	os.MkdirAll(destDir, 0755)
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		path := filepath.Join(destDir, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(path, 0755)
			continue
		}
		os.MkdirAll(filepath.Dir(path), 0755)

		rc, err := f.Open()
		if err != nil {
			return err
		}
		out, err := os.Create(path)
		if err != nil {
			rc.Close()
			return err
		}
		_, err = io.Copy(out, rc)
		out.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
