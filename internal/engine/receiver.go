package engine

import (
	"archive/zip"
	"context"
	"encoding/binary"
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
	"comet/internal/zipper"
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
	parts := strings.SplitN(meta, "|", 4)
	if len(parts) < 3 {
		conn.SendPacket(network.CmdError, []byte("元数据格式错误"))
		return
	}
	fileName := parts[0]
	totalSize, _ := strconv.ParseInt(parts[1], 10, 64)
	sessionID := parts[2]
	isFolder := len(parts) >= 4 && parts[3] == "1"

	r.logger.Infof("[Receiver] 接收: %s, 大小: %d, session: %s, isFolder: %t", fileName, totalSize, sessionID, isFolder)

	// 3. 确认元数据
	conn.SendPacket(network.CmdMetaAck, nil)

	// 4. 断点续传 - 与发送端交换位图
	totalChunks := int((totalSize + r.chunkSize - 1) / r.chunkSize)

	cp, _ := r.store.LoadCheckpoint(sessionID)
	if cp != nil {
		r.logger.Infof("[Receiver] 发现本地断点记录, 已完成 %d/%d 块", countTrue(cp.Completed), cp.TotalChunks)
	}

	// 等待发送端的查询，回复本地位图
	cmd, payload, err = conn.ReadPacket()
	if err != nil {
		r.logger.Errorf("[Receiver] 读取查询失败: %v", err)
		return
	}
	if cmd != network.CmdQuery {
		conn.SendPacket(network.CmdError, []byte("期望QUERY"))
		return
	}

	localCompleted := []bool{}
	if cp != nil {
		localCompleted = cp.Completed
	}
	if err := conn.SendPacket(network.CmdQueryResp, boolsToBytes(localCompleted)); err != nil {
		r.logger.Errorf("[Receiver] 发送位图失败: %v", err)
		return
	}

	// 等待发送端返回合并后的位图
	cmd, payload, err = conn.ReadPacket()
	if err != nil {
		r.logger.Errorf("[Receiver] 读取合并位图失败: %v", err)
		return
	}
	if cmd != network.CmdChkSync {
		conn.SendPacket(network.CmdError, []byte("期望CHKSYNC"))
		return
	}

	completed := bytesToBools(payload)
	if len(completed) == 0 {
		completed = make([]bool, totalChunks)
	}

	// 保存合并后的位图
	r.store.UpdateCheckpoint(sessionID, func(cp *models.Checkpoint) {
		c := make([]bool, len(completed))
		copy(c, completed)
		cp.Completed = c
	})
	r.logger.Infof("[Receiver] 断点合并后: 已完成 %d/%d 块", countTrue(completed), totalChunks)

	// 准备保存路径：downloads/{sessionID}/
	sessionDir := filepath.Join(r.saveDir, sessionID)
	os.MkdirAll(sessionDir, 0755)
	savePath := filepath.Join(sessionDir, fileName)

	// 5. 接收分块
	var transferred int64
	r.logger.Infof("[Receiver] 总块: %d 总大小: %d ", totalChunks, totalSize)

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

		if len(payload) < 8 {
			r.logger.Warnf("[Receiver] 分块数据太短: %d", len(payload))
			continue
		}

		idx := int(binary.BigEndian.Uint32(payload[:4]))
		dataLen := int(binary.BigEndian.Uint32(payload[4:8]))

		if len(payload) < 8+dataLen {
			r.logger.Warnf("[Receiver] 分块数据长度不匹配: 期望 %d, 实际 %d", dataLen, len(payload)-8)
			continue
		}

		data := payload[8 : 8+dataLen]

		// 写入文件
		offset := int64(idx) * r.chunkSize
		if _, err := f.WriteAt(data, offset); err != nil {
			r.logger.Errorf("[Receiver] 写入失败: %v", err)
			continue
		}
		r.logger.Debugf("[Receiver] idx: %d", idx)
		completed[idx] = true
		atomic.AddInt64(&transferred, int64(len(data)))

		// 定期保存进度（每10块保存一次）
		if atomic.LoadInt64(&transferred)%(r.chunkSize*10) < r.chunkSize {
			r.store.UpdateCheckpoint(sessionID, func(cp *models.Checkpoint) {
				c := make([]bool, len(completed))
				copy(c, completed)
				cp.Completed = c
			})
		}

		if r.progressFunc != nil {
			r.progressFunc(sessionID, transferred, totalSize)
		}
		r.logger.Debugf("[Receiver] 分块 %d 已接收", idx)
	}

	// 6. 保存最终进度，重命名临时文件，解压到 session 目录
	r.store.UpdateCheckpoint(sessionID, func(cp *models.Checkpoint) {
		c := make([]bool, len(completed))
		copy(c, completed)
		cp.Completed = c
	})
	f.Close()

	// 临时文件 → 正式文件（Windows rename 不覆盖，需先删除目标）
	os.Remove(savePath)
	if err := os.Rename(tmpFile, savePath); err != nil {
		r.logger.Errorf("[Receiver] 重命名文件失败: %v", err)
		return
	}

	if isFolder {
		// 解压到 sessionDir（如 downloads/{sessionID}/）
		z := zipper.NewZstdZipper(0)
		r.logger.Infof("[Receiver] 解压到: %s", sessionDir)

		if err := z.Unpack(savePath, sessionDir); err != nil {
			r.logger.Errorf("[Receiver] 解压失败: %v", err)
			return
		}

		// 删除收到的压缩包
		os.Remove(savePath)
	} else {
		r.logger.Infof("[Receiver] 收到普通文件: %s", savePath)
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
