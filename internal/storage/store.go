package storage

import "comet/internal/models"

type Store interface {
	// Task operations
	SaveTask(task *models.Task) error
	LoadTask(taskID string) (*models.Task, error)
	ListTasks() ([]*models.Task, error)
	DeleteTask(taskID string) error

	// SendTask operations
	SaveSendTask(task *models.SendTask) error
	LoadSendTask(taskID string) (*models.SendTask, error)
	ListSendTasks() ([]*models.SendTask, error)
	DeleteSendTask(taskID string) error

	// ReceiveTask operations
	SaveReceiveTask(task *models.ReceiveTask) error
	LoadReceiveTask(taskID string) (*models.ReceiveTask, error)
	ListReceiveTasks() ([]*models.ReceiveTask, error)
	DeleteReceiveTask(taskID string) error

	// Checkpoint operations
	SaveCheckpoint(sessionID string, cp *models.Checkpoint) error
	LoadCheckpoint(sessionID string) (*models.Checkpoint, error)
	DeleteCheckpoint(sessionID string) error

	// Peer operations
	SavePeer(peer *models.Peer) error
	LoadPeer(peerID string) (*models.Peer, error)
	ListPeers() ([]*models.Peer, error)
	DeletePeer(peerID string) error
}
