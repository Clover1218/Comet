package models

import "time"

type Checkpoint struct {
	SessionID   string    `json:"session_id"`
	TotalChunks int       `json:"total_chunks"`
	ChunkSize   int64     `json:"chunk_size"`
	Completed   []bool    `json:"completed"`
	UpdatedAt   time.Time `json:"updated_at"`
	FilePath    string    `json:"file_path"`
	IsFoler     bool      `json:"is_folder"` // 0-文件 1-文件夹
	Type        int       `json:"type"`      // 0-发送请求 1-接收请求
	Finished    bool      `json:"finished"`  // 是否完成
	TargetAddr  string    `json:"target_addr"`
}
