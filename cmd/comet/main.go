package main

import (
	"bufio"
	"comet/internal/config"
	"comet/internal/engine"
	"comet/internal/logger"
	"comet/internal/network"
	"comet/internal/peer"
	"comet/internal/storage"
	"comet/internal/task"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// sanitizePath 移除路径中混入的不可见 Unicode 控制字符（如 U+202A 等）
// 这些字符常从文件资源管理器或其他应用复制路径时混入，会导致 CreateFile 失败
func sanitizePath(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, s)
}

func runCLI(ctx context.Context, cfg *config.Config, store storage.Store, peerMgr peer.Manager,
	taskMgr *task.TaskManager, sender *engine.Sender, receiver *engine.Receiver, logger *logger.Logger,
) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("\n= Comet =")
	fmt.Println("输入 help 查看命令")
	fmt.Print("\nComet> ")

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			fmt.Print("Comet> ")
			continue
		}

		args := strings.Fields(input)
		if len(args) == 0 {
			fmt.Print("Comet> ")
			continue
		}
		cmd := args[0]

		switch cmd {
		case "help":
			printHelp()

		case "peers":
			peers, err := peerMgr.ListPeers()
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else if len(peers) == 0 {
				fmt.Println("没有在线节点")
			} else {
				fmt.Println("在线节点:")
				for i, p := range peers {
					fmt.Printf("  [%d] %s (%s) 延迟: %v\n", i+1, p.Hostname, p.Addr, time.Since(p.LastSeen).Round(time.Millisecond))
				}
			}

		case "peer":
			if len(args) < 2 {
				fmt.Println("用法: peer add <name> <addr> 或 peer remove <id>")
				fmt.Print("Comet> ")
				continue
			}
			subCmd := args[1]
			switch subCmd {
			case "add":
				if len(args) < 4 {
					fmt.Println("用法: peer add <name> <addr:port>")
					break
				}
				name := args[2]
				addr := args[3]
				if err := peerMgr.AddManualPeer(name, addr); err != nil {
					fmt.Printf("添加失败: %v\n", err)
				} else {
					fmt.Printf("✅ 手动节点添加成功: %s (%s)\n", name, addr)
				}
			case "remove":
				if len(args) < 3 {
					fmt.Println("用法: peer remove <id>")
					break
				}
				id := args[2]
				if err := peerMgr.RemovePeer(id); err != nil {
					fmt.Printf("移除失败: %v\n", err)
				} else {
					fmt.Printf("✅ 节点已移除: %s\n", id)
				}
			default:
				fmt.Printf("未知子命令: %s\n", subCmd)
			}

		case "task":
			if len(args) < 2 {
				fmt.Println("用法: task create <path> | task list | task delete <id>")
				fmt.Print("Comet> ")
				continue
			}
			subCmd := args[1]
			switch subCmd {
			case "create":
				if len(args) < 3 {
					fmt.Println("用法: task create <path>")
					break
				}
				path := sanitizePath(args[2])
				task, err := taskMgr.CreateTaskFromPath(path)
				if err != nil {
					fmt.Printf("创建任务失败: %v\n", err)
				} else {
					fmt.Printf("✅ 任务创建成功: %s (ID: %s, 大小: %.2f MB, 文件: %d)\n",
						task.Name, task.TaskID, float64(task.TotalSize)/1024/1024, task.FileCount)
				}
			case "list":
				tasks, err := taskMgr.ListTasks()
				if err != nil {
					fmt.Printf("获取任务列表失败: %v\n", err)
				} else if len(tasks) == 0 {
					fmt.Println("没有任务")
				} else {
					fmt.Println("任务列表:")
					for i, t := range tasks {
						fmt.Printf("  [%d] %s (%s) %.2f MB %d文件 创建于 %s\n",
							i+1, t.Name, t.TaskID, float64(t.TotalSize)/1024/1024, t.FileCount,
							t.CreatedAt.Format("15:04:05"))
					}
				}
			case "delete":
				if len(args) < 3 {
					fmt.Println("用法: task delete <id>")
					break
				}
				id := args[2]
				if err := taskMgr.DeleteTask(id); err != nil {
					fmt.Printf("删除失败: %v\n", err)
				} else {
					fmt.Printf("✅ 任务已删除: %s\n", id)
				}
			default:
				fmt.Printf("未知子命令: %s\n", subCmd)
			}

		case "send":
			if len(args) < 3 {
				fmt.Println("用法: send <task_id或路径> <目标地址或节点序号>")
				fmt.Print("Comet> ")
				continue
			}
			target := args[2]
			// 判断目标是否是序号
			var targetAddr string
			if idx, err := strconv.Atoi(target); err == nil {
				peers, _ := peerMgr.ListPeers()
				if idx >= 1 && idx <= len(peers) {
					targetAddr = peers[idx-1].Addr
				} else {
					fmt.Printf("错误: 节点序号 %d 无效\n", idx)
					fmt.Print("Comet> ")
					continue
				}
			} else {
				targetAddr = target
			}
			if targetAddr == "" {
				fmt.Println("错误: 无法解析目标地址")
				fmt.Print("Comet> ")
				continue
			}

			// 判断第一个参数是Task ID还是路径
			src := sanitizePath(args[1])
			task, err := taskMgr.GetTask(src)
			var path string
			var isFolder bool
			if err == nil && task != nil {
				path = task.SourcePath
				info, err := os.Stat(path)
				if err != nil {
					fmt.Printf("错误: %v\n", err)
					fmt.Print("Comet> ")
				}
				isFolder = info.IsDir()
				fmt.Printf("使用任务 '%s' 发送 : %s\n", task.Name, path)
			} else {
				// 当作直接路径处理
				path = sanitizePath(args[1])
				info, err := os.Stat(path)
				if err != nil {
					fmt.Printf("错误: %v\n", err)
					fmt.Print("Comet> ")
					continue
				}
				isFolder = info.IsDir()
			}

			fmt.Printf("发送 %s 到 %s ...\n", path, targetAddr)

			go func() {
				var err error
				if isFolder {
					err = sender.SendFolder(ctx, path, targetAddr)
				} else {
					err = sender.SendFile(ctx, path, targetAddr)
				}
				if err != nil {
					logger.Errorf("发送失败: %v", err)
					fmt.Printf("\n❌ 发送失败: %v\n", err)
				} else {
					fmt.Println("\n✅ 发送完成!")
				}
				fmt.Print("Comet> ")
			}()
		case "checkpoints":
			if len(args) < 2 {
				fmt.Println("用法: checkpoints continue <id> | checkpoints list")
				fmt.Print("Comet> ")
				continue
			}
			subCmd := args[1]
			switch subCmd {
			case "continue":
				if len(args) < 4 {
					fmt.Println("用法: checkpoints continue <id> <address>")
					break
				}
				sessionID := sanitizePath(args[2])

				checkpoint, err := store.LoadCheckpoint(sessionID)
				if err != nil {
					fmt.Printf("未找到该检查点: %v\n", err)
					fmt.Print("Comet> ")
					continue
				}
				addr := sanitizePath(args[3])
				if addr == "" {
					addr = checkpoint.TargetAddr
				}
				fmt.Printf("找到检查点，继续：")
				go func() {
					var err error
					if !checkpoint.IsFoler {
						err = sender.SendFileBySession(ctx, checkpoint.FilePath, addr, sessionID)
					} else {
						err = sender.SendFolderBySession(ctx, checkpoint.FilePath, addr, sessionID)
					}
					if err != nil {
						fmt.Printf("继续检查点失败: %v\n", err)
					}
				}()

			case "list":
				checkpoints, err := store.ListCheckpoint()
				if err != nil {
					fmt.Printf("获取检查点列表失败: %v\n", err)
				} else if len(checkpoints) == 0 {
					fmt.Println("没有检查点")
				} else {
					fmt.Println("检查点列表:")

					for i, t := range checkpoints {
						var typeStr string
						if t.Type == 0 {
							typeStr = "发送请求"
						} else {
							typeStr = "接收请求"
						}
						fmt.Printf("  [%d] %s \n  %s 进度(%d/%d) 最后更新于 %s\n",
							i+1, t.SessionID, typeStr, t.TotalChunks, t.TotalChunks,
							t.UpdatedAt.Format("15:04:05"))
					}
				}

			default:
				fmt.Printf("未知子命令: %s\n", subCmd)
			}
		// case "receive":
		// 	if len(args) < 2 {
		// 		fmt.Println("用法: receive list | receive accept <session-id> | receive reject <session-id>")
		// 		fmt.Print("Comet> ")
		// 		continue
		// 	}
		// 	subCmd := args[1]
		// 	switch subCmd {
		// 	case "list":
		// 		reqs := receiver.ListRequests()
		// 		if len(reqs) == 0 {
		// 			fmt.Println("没有待处理的接收请求")
		// 		} else {
		// 			fmt.Println("待处理接收请求:")
		// 			for i, req := range reqs {
		// 				fmt.Printf("  [%d] %s (%.2f MB) 来自 %s 状态: %s\n",
		// 					i+1, req.FileName, float64(req.TotalSize)/1024/1024, req.PeerAddr, req.Status)
		// 			}
		// 		}
		// 	case "accept":
		// 		if len(args) < 3 {
		// 			fmt.Println("用法: receive accept <session-id>")
		// 			break
		// 		}
		// 		sessionID := args[2]
		// 		if err := receiver.AcceptRequest(sessionID); err != nil {
		// 			fmt.Printf("接受失败: %v\n", err)
		// 		} else {
		// 			fmt.Printf("✅ 已接受请求 %s，开始接收...\n", sessionID)
		// 		}
		// 	case "reject":
		// 		if len(args) < 3 {
		// 			fmt.Println("用法: receive reject <session-id>")
		// 			break
		// 		}
		// 		sessionID := args[2]
		// 		if err := receiver.RejectRequest(sessionID); err != nil {
		// 			fmt.Printf("拒绝失败: %v\n", err)
		// 		} else {
		// 			fmt.Printf("❌ 已拒绝请求 %s\n", sessionID)
		// 		}
		// 	default:
		// 		fmt.Printf("未知子命令: %s\n", subCmd)
		// 	}

		case "exit", "quit":
			fmt.Println("退出...")
			return

		default:
			fmt.Printf("未知命令: %s, 输入 help 查看帮助\n", cmd)
		}
		fmt.Print("Comet> ")
	}
}

func printHelp() {
	fmt.Println(`
命令:
  peers                            查看在线节点
  peer add <name> <addr:port>     手动添加节点
  peer remove <id>                移除手动节点
  task create <path>              创建传输任务（扫描文件夹）
  task list                       列出所有任务
  task delete <id>                删除任务
  send <task_id或路径> <目标>     发送文件/文件夹（目标可为地址或节点序号）
  receive list                    查看待处理的接收请求
  receive accept <session-id>     接受接收请求
  receive reject <session-id>     拒绝接收请求
  help                            显示帮助
  exit/quit                       退出程序

示例:
  peer add mypc [2001:db8::2]:9000
  task create ~/photos
  task list
  send abc123 1                    (发送任务给序号1的节点)
  send ./report.pdf [2001:db8::2]:9000
  receive list
  receive accept session-xxx

`)
	return
}
func main() {
	if err := config.InitConfig("./configs/config.yaml", "yaml"); err != nil {
		panic(err)
	}
	log, err := logger.New(logger.Config{
		Dir:       "./logs",
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	if err != nil {
		panic(err)
	}
	store := storage.NewFileStore(config.GlobalConfig.Storage.DataDir)
	peerMgr := peer.NewPeerManager(
		log,
		store,
		config.GlobalConfig.Node.ID,
		config.GlobalConfig.Network.Port,
		config.GlobalConfig.Network.InterfaceName,
	)
	taskMgr := task.NewTaskManager(store)
	// 启动发现
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := peerMgr.Start(ctx); err != nil {
		log.Errorf("启动节点发现失败: %v", err)
	}
	defer peerMgr.Stop()
	transport := network.NewTCPTransport()
	sender := engine.NewSender(
		transport,
		store,
		config.GlobalConfig.Transfer.ChunkSize,
		config.GlobalConfig.Transfer.MaxWorkers,
		config.GlobalConfig.Transfer.Timeout,
		config.GlobalConfig.Security.Token,
		log,
	)

	receiver := engine.NewReceiver(
		transport,
		store,
		filepath.Join(config.GlobalConfig.Storage.DataDir, config.GlobalConfig.Storage.DownloadsDir),
		config.GlobalConfig.Transfer.ChunkSize,
		config.GlobalConfig.Transfer.Timeout,
		config.GlobalConfig.Security.Token,
		log,
	)

	// 7. 启动接收服务
	go func() {
		listenAddr := fmt.Sprintf("%s:%d", config.GlobalConfig.Network.ListenAddr, config.GlobalConfig.Network.Port)
		if err := receiver.Start(ctx, listenAddr); err != nil && err != context.Canceled {
			log.Errorf("接收服务错误: %v", err)
		}
	}()
	// 监听节点变化事件（可用于 CLI 提示）
	go func() {
		for event := range peerMgr.Events() {
			switch event.Type {
			case peer.EventPeerJoined:
				log.Infof("🎉 [UI] %s 上线了！", event.Peer.Hostname)
			case peer.EventPeerLeft:
				log.Infof("👋 [UI] %s 下线了", event.Peer.Hostname)
			}
		}
	}()
	runCLI(ctx, config.GlobalConfig, store, peerMgr, taskMgr, sender, receiver, log)

}
