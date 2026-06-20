package task

import (
	"comet/internal/models"
	"comet/internal/storage"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type TaskManager struct {
	store    storage.Store
	mu       sync.RWMutex
	maxIndex int
}

func NewTaskManager(store storage.Store) *TaskManager {
	return &TaskManager{store: store,
		maxIndex: -1}
}

// CreateTaskFromPath 扫描路径创建Task并保存
func (tm *TaskManager) CreateTaskFromPath(path string) (*models.Task, error) {
	task, err := createTask(path, strconv.Itoa(tm.maxIndex+1))
	if err != nil {
		return nil, err
	}
	if err := tm.store.SaveTask(task); err != nil {
		return nil, err
	}
	return task, nil
}

// ListTasks 列出所有已保存的Task
func (tm *TaskManager) ListTasks() ([]*models.Task, error) {
	tasks, err := tm.store.ListTasks()
	if err != nil {
		return nil, err
	}
	if tm.maxIndex == -1 {
		tmpIndex := tm.maxIndex
		for _, task := range tasks {
			taskID, err := strconv.Atoi(task.TaskID)
			if err != nil {
				continue
			}
			tmpIndex = max(tmpIndex, taskID)
		}
		tm.maxIndex = tmpIndex
	}

	return tasks, err
}

// GetTask 获取Task详情
func (tm *TaskManager) GetTask(id string) (*models.Task, error) {
	return tm.store.LoadTask(id)
}

// DeleteTask 删除Task
func (tm *TaskManager) DeleteTask(id string) error {
	return tm.store.DeleteTask(id)
}

func createTask(path string, ID string) (*models.Task, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	// 2. 检查路径是否存在并获取文件信息
	info, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}

	// 3. 生成唯一任务ID（使用加密随机数，失败时回退到时间戳）
	taskID := ID

	// 4. 统计文件数量和总大小
	var totalSize int64
	var fileCount int

	if info.IsDir() {
		// 目录：递归遍历所有子文件
		err = filepath.Walk(absPath, func(filePath string, fileInfo os.FileInfo, walkErr error) error {
			if walkErr != nil {
				// 如果某个文件无法访问，可选择跳过或终止；这里选择直接返回错误
				return walkErr
			}
			if !fileInfo.IsDir() {
				totalSize += fileInfo.Size()
				fileCount++
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		// 单个文件
		totalSize = info.Size()
		fileCount = 1
	}

	// 5. 构造Task对象
	task := &models.Task{
		TaskID:     taskID,
		Name:       filepath.Base(absPath), // 取路径最后一部分作为任务名称
		SourcePath: absPath,
		TotalSize:  totalSize,
		FileCount:  fileCount,
		CreatedAt:  time.Now(),
	}

	return task, nil
}
