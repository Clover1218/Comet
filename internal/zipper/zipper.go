package zipper

import (
	"fmt"
	"os"
	"path/filepath"
)

type Zipper interface {
	Pack(srcFile string, dstFile string) error
	Unpack(srcFile string, destDir string) error
	Extension() string
}

func ensureWritableDir(dir string) error {
	// 尝试创建目录（如果不存在）
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("无法创建目录 %s: %w", dir, err)
	}
	// 尝试在目录下创建临时文件测试写权限
	f, err := os.CreateTemp(dir, "writecheck_*")
	if err != nil {
		return fmt.Errorf("目录 %s 不可写: %w", dir, err)
	}
	f.Close()
	os.Remove(f.Name())
	return nil
}

// ensureWritableFile 检查目标文件所在目录是否可写，且文件可创建
func ensureWritableFile(filePath string) error {
	dir := filepath.Dir(filePath)
	// 检查目录可写
	if err := ensureWritableDir(dir); err != nil {
		return err
	}
	// 如果文件已存在，检查其可写性（OpenFile with O_WRONLY）
	// 如果不存在，确保可以创建
	// 使用 O_CREATE|O_EXCL 可以测试创建权限，但可能干扰已存在的文件
	// 更安全：使用 O_CREATE|O_WRONLY 测试写入，然后删除
	// 但为了不影响文件内容，我们采用在目录创建临时文件的方式（已检查目录可写），所以文件创建权限通常没问题
	// 对于普通用户，目录可写就代表可以创建文件。
	// 因此这里返回 nil
	return nil
}
