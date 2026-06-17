package models

import "time"

type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusPaused    TaskStatus = "paused"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusRejected  TaskStatus = "rejected"
)

type Task struct {
	TaskID     string    `json:"task_id"`
	Name       string    `json:"name"`
	SourcePath string    `json:"source_path"`
	TotalSize  int64     `json:"total_size"`
	FileCount  int       `json:"file_count"`
	CreatedAt  time.Time `json:"created_at"`
}

type SendTask struct {
	TaskID         string     `json:"task_id"`
	PeerID         string     `json:"peer_id"`
	Status         TaskStatus `json:"status"`
	Progress       float64    `json:"progress"`
	Transferred    int64      `json:"transferred"`
	StartTime      time.Time  `json:"start_time"`
	UpdateTime     time.Time  `json:"update_time"`
	CheckpointFile string     `json:"checkpoint_file"`
}

type ReceiveTask struct {
	TaskID     string     `json:"task_id"`
	TaskName   string     `json:"task_name"`
	PeerID     string     `json:"peer_id"`
	SaveDir    string     `json:"save_dir"`
	TotalSize  int64      `json:"total_size"`
	Status     TaskStatus `json:"status"`
	Progress   float64    `json:"progress"`
	Checkpoint string     `json:"checkpoint"`
}

type Checkpoint struct {
	SessionID   string    `json:"session_id"`
	TotalChunks int       `json:"total_chunks"`
	ChunkSize   int64     `json:"chunk_size"`
	Completed   []bool    `json:"completed"`
	UpdatedAt   time.Time `json:"updated_at"`
}
