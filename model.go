package comet

// import (
// 	"context"
// 	"io"
// 	"net"
// 	"time"
// )

// type Task struct {
// 	ID         string    `json:"id"`          // UUID
// 	Name       string    `json:"name"`        // 用户友好的任务名（默认文件夹名）
// 	SourcePath string    `json:"source_path"` // 本地源文件夹路径
// 	TotalSize  int64     `json:"total_size"`  // 打包后总大小（估算）
// 	FileCount  int       `json:"file_count"`  // 文件夹内文件数
// 	CreatedAt  time.Time `json:"created_at"`
// }
// type TaskStatus string

// const (
// 	StatusPending   TaskStatus = "pending"   // 等待传输
// 	StatusRunning   TaskStatus = "running"   // 传输中
// 	StatusPaused    TaskStatus = "paused"    // 已暂停
// 	StatusCompleted TaskStatus = "completed" // 已完成
// 	StatusFailed    TaskStatus = "failed"    // 失败
// 	StatusRejected  TaskStatus = "rejected"  // 被接收方拒绝
// )

// type SendTask struct {
// 	Task           *Task      `json:"task"`
// 	TargetPeer     *Peer      `json:"target_peer"`
// 	Status         TaskStatus `json:"status"`      // pending/running/paused/completed/failed
// 	Progress       float64    `json:"progress"`    // 0.0 ~ 1.0
// 	Transferred    int64      `json:"transferred"` // 已传字节数
// 	StartTime      time.Time  `json:"start_time"`
// 	UpdateTime     time.Time  `json:"update_time"`
// 	CheckpointFile string     `json:"checkpoint_file"` // 断点记录文件路径
// }
// type ReceiveTask struct {
// 	TaskID         string     `json:"task_id"`
// 	TaskName       string     `json:"task_name"` // 原始任务名
// 	SenderPeer     *Peer      `json:"sender_peer"`
// 	SaveDir        string     `json:"save_dir"` // 实际保存路径（含UUID防重名）
// 	TotalSize      int64      `json:"total_size"`
// 	Status         TaskStatus `json:"status"`
// 	Progress       float64    `json:"progress"`
// 	CheckpointFile string     `json:"checkpoint_file"`
// }
// type TaskManager interface {
// 	// CreateTask 扫描文件夹，创建一个Task（不传输）
// 	CreateTask(sourcePath string) (*Task, error)

// 	// GetTask 根据ID获取Task
// 	GetTask(taskID string) (*Task, error)

// 	// ListTasks 列出所有已创建的Task
// 	ListTasks() ([]*Task, error)

// 	// DeleteTask 删除Task（仅删除元数据，不影响源文件）
// 	DeleteTask(taskID string) error
// }
// type SendQueue interface {
// 	// CreateSendTask 创建一个发送任务（Task + Peer → SendTask）
// 	CreateSendTask(taskID string, peer *Peer) (*SendTask, error)

// 	// StartSend 开始执行发送任务（启动传输协程）
// 	StartSend(sendTaskID string) error

// 	// PauseSend 暂停发送任务
// 	PauseSend(sendTaskID string) error

// 	// ResumeSend 恢复发送任务（断点续传）
// 	ResumeSend(sendTaskID string) error

// 	// GetSendTask 查询发送任务状态
// 	GetSendTask(sendTaskID string) (*SendTask, error)

// 	// ListSendTasks 列出所有发送任务
// 	ListSendTasks() ([]*SendTask, error)
// }
// type ReceiveQueue interface {
// 	// OnReceiveRequest 收到发送端的传输请求时调用
// 	// 返回: accept 或 reject，以及拒绝原因
// 	OnReceiveRequest(req *ReceiveRequest) (*ReceiveDecision, error)

// 	// AcceptReceive 用户接受请求后，执行接收
// 	AcceptReceive(receiveTaskID string) error

// 	// RejectReceive 用户拒绝请求
// 	RejectReceive(receiveTaskID string, reason string) error

// 	// GetReceiveTask 查询接收任务
// 	GetReceiveTask(receiveTaskID string) (*ReceiveTask, error)

// 	// ListReceiveTasks 列出所有接收任务
// 	ListReceiveTasks() ([]*ReceiveTask, error)
// }
// type TransferEngine interface {
// 	// SendStream 发送一个数据流（核心方法，被SendQueue调用）
// 	// 参数: 数据流Reader、总大小、目标Peer、会话ID、进度回调
// 	SendStream(
// 		ctx context.Context,
// 		reader io.Reader,
// 		totalSize int64,
// 		targetPeer *Peer,
// 		sessionID string,
// 		progressCallback func(transferred int64),
// 	) error

// 	// ReceiveStream 接收一个数据流（核心方法，被ReceiveQueue调用）
// 	ReceiveStream(
// 		ctx context.Context,
// 		conn net.Conn,
// 		sessionID string,
// 		savePath string,
// 		totalSize int64,
// 		progressCallback func(transferred int64),
// 	) error
// }

// // Checkpoint 断点记录（存 checkpoints/checkpoint_xxx.chk）
// type Checkpoint struct {
// 	SessionID   string    `json:"session_id"`
// 	TotalChunks int       `json:"total_chunks"`
// 	ChunkSize   int64     `json:"chunk_size"`
// 	Completed   []bool    `json:"completed"` // 长度 = TotalChunks
// 	UpdatedAt   time.Time `json:"updated_at"`
// }

// // 原子保存（沿用之前的三步法：临时文件 → Sync → Rename）
// func (c *Checkpoint) Save(dir string) error {
// 	// ...
// }
