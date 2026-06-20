package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"comet/internal/models"
)

type FileStore struct {
	baseDir string
	mu      sync.RWMutex
}

func NewFileStore(dataDir string) *FileStore {
	dirs := []string{
		filepath.Join(dataDir, "tasks"),
		filepath.Join(dataDir, "queue", "send"),
		filepath.Join(dataDir, "queue", "receive"),
		filepath.Join(dataDir, "checkpoints"),
		filepath.Join(dataDir, "peers"),
		filepath.Join(dataDir, "downloads"),
	}
	for _, d := range dirs {
		os.MkdirAll(d, 0755)
	}
	return &FileStore{baseDir: dataDir}
}

func (fs *FileStore) atomicSave(dir, filename string, data interface{}) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	finalPath := filepath.Join(dir, filename)
	tmpPath := finalPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Windows: os.Rename does not overwrite existing files, remove dest first
	os.Remove(finalPath)
	return os.Rename(tmpPath, finalPath)
}

func (fs *FileStore) atomicLoad(dir, filename string, data interface{}) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	path := filepath.Join(dir, filename)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	return json.NewDecoder(f).Decode(data)
}

// Task operations
func (fs *FileStore) SaveTask(task *models.Task) error {
	dir := filepath.Join(fs.baseDir, "tasks")
	return fs.atomicSave(dir, task.TaskID+".json", task)
}

func (fs *FileStore) LoadTask(taskID string) (*models.Task, error) {
	dir := filepath.Join(fs.baseDir, "tasks")
	var task models.Task
	if err := fs.atomicLoad(dir, taskID+".json", &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (fs *FileStore) ListTasks() ([]*models.Task, error) {
	dir := filepath.Join(fs.baseDir, "tasks")
	return listJSONFiles[models.Task](fs, dir)
}

func (fs *FileStore) DeleteTask(taskID string) error {
	path := filepath.Join(fs.baseDir, "tasks", taskID+".json")
	return os.Remove(path)
}

// Checkpoint operations
func (fs *FileStore) SaveCheckpoint(sessionID string, cp *models.Checkpoint) error {
	dir := filepath.Join(fs.baseDir, "checkpoints")
	return fs.atomicSave(dir, sessionID+".json", cp)
}

func (fs *FileStore) LoadCheckpoint(sessionID string) (*models.Checkpoint, error) {
	dir := filepath.Join(fs.baseDir, "checkpoints")
	var cp models.Checkpoint
	if err := fs.atomicLoad(dir, sessionID+".json", &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func (fs *FileStore) DeleteCheckpoint(sessionID string) error {
	path := filepath.Join(fs.baseDir, "checkpoints", sessionID+".json")
	return os.Remove(path)
}

// UpdateCheckpoint 增量更新 checkpoint：加载已有数据，调用 updateFn 修改，更新 UpdatedAt 后保存
func (fs *FileStore) UpdateCheckpoint(sessionID string, updateFn func(*models.Checkpoint)) (*models.Checkpoint, error) {
	cp, err := fs.LoadCheckpoint(sessionID)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, os.ErrNotExist
	}

	updateFn(cp)
	cp.UpdatedAt = time.Now()

	dir := filepath.Join(fs.baseDir, "checkpoints")
	if err := fs.atomicSave(dir, sessionID+".json", cp); err != nil {
		return nil, err
	}
	return cp, nil
}
func (fs *FileStore) ListCheckpoint() ([]*models.Checkpoint, error) {
	dir := filepath.Join(fs.baseDir, "checkpoints")
	return listJSONFiles[models.Checkpoint](fs, dir)
}

// Peer operations
func (fs *FileStore) SavePeer(peer *models.Peer) error {
	dir := filepath.Join(fs.baseDir, "peers")
	return fs.atomicSave(dir, peer.ID+".json", peer)
}

func (fs *FileStore) LoadPeer(peerID string) (*models.Peer, error) {
	dir := filepath.Join(fs.baseDir, "peers")
	var peer models.Peer
	if err := fs.atomicLoad(dir, peerID+".json", &peer); err != nil {
		return nil, err
	}
	return &peer, nil
}

func (fs *FileStore) ListPeers() ([]*models.Peer, error) {
	dir := filepath.Join(fs.baseDir, "peers")
	return listJSONFiles[models.Peer](fs, dir)
}

func (fs *FileStore) DeletePeer(peerID string) error {
	path := filepath.Join(fs.baseDir, "peers", peerID+".json")
	return os.Remove(path)
}

// Generic helper
func listJSONFiles[T any](fs *FileStore, dir string) ([]*T, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*T{}, nil
		}
		return nil, err
	}

	result := []*T{}
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".json" {
			continue
		}
		item := new(T)
		if err := fs.atomicLoad(dir, f.Name(), item); err != nil {
			continue
		}
		result = append(result, item)
	}
	return result, nil
}
