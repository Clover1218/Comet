package zipper

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

type ZstdZipper struct {
	level zstd.EncoderLevel
}

func NewZstdZipper(level zstd.EncoderLevel) *ZstdZipper {
	if level == 0 {
		level = zstd.SpeedFastest
	}
	return &ZstdZipper{level: level}
}

func (z *ZstdZipper) Pack(srcDir string, dstFile string) error {
	if err := ensureWritableFile(dstFile); err != nil {
		return err
	}

	f, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer f.Close()

	enc, err := zstd.NewWriter(f, zstd.WithEncoderLevel(z.level))
	if err != nil {
		return err
	}
	defer enc.Close()

	tw := tar.NewWriter(enc)
	defer tw.Close()

	// 使用源目录名作为 tar 中的顶层目录前缀
	dirName := filepath.Base(srcDir)

	// 先写入目录自身
	dirInfo, err := os.Stat(srcDir)
	if err != nil {
		return err
	}
	tw.WriteHeader(&tar.Header{
		Name:     dirName + "/",
		Typeflag: tar.TypeDir,
		Mode:     int64(dirInfo.Mode() & 0777),
		ModTime:  dirInfo.ModTime(),
	})

	return filepath.WalkDir(srcDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(filepath.Join(dirName, relPath))

		info, err := d.Info()
		if err != nil {
			return err
		}
		header := &tar.Header{
			Name:    relPath,
			Size:    info.Size(),
			Mode:    int64(info.Mode() & 0777),
			ModTime: info.ModTime(),
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(tw, file)
		return err
	})
}

func (z *ZstdZipper) Unpack(srcFile string, destDir string) error {
	if err := ensureWritableDir(destDir); err != nil {
		return err
	}

	f, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer f.Close()

	dec, err := zstd.NewReader(f)
	if err != nil {
		return err
	}
	defer dec.Close()

	tr := tar.NewReader(dec)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		target := filepath.Join(destDir, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			out, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			out.Close()
			if err := os.Chmod(target, os.FileMode(header.Mode)); err != nil {
				// 仅记录日志，不中断流程
			}
		}
	}
	return nil
}

func (z *ZstdZipper) Extension() string {
	return "tar.zst"
}
