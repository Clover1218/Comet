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
