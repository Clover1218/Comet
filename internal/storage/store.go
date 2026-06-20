package storage

import "comet/internal/models"

type Store interface {
	// Task operations
	SaveTask(task *models.Task) error
	LoadTask(taskID string) (*models.Task, error)
	ListTasks() ([]*models.Task, error)
	DeleteTask(taskID string) error

	// Checkpoint operations
	SaveCheckpoint(sessionID string, cp *models.Checkpoint) error
	LoadCheckpoint(sessionID string) (*models.Checkpoint, error)
	DeleteCheckpoint(sessionID string) error
	ListCheckpoint() ([]*models.Checkpoint, error)
	UpdateCheckpoint(sessionID string, updateFn func(*models.Checkpoint)) (*models.Checkpoint, error)

	// Peer operations
	SavePeer(peer *models.Peer) error
	LoadPeer(peerID string) (*models.Peer, error)
	ListPeers() ([]*models.Peer, error)
	DeletePeer(peerID string) error
}
