package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// dailyWriter 实现 io.Writer，按天切割文件，支持懒加载
type dailyWriter struct {
	dir       string
	curDate   string
	file      *os.File
	mu        sync.Mutex
	initOnce  sync.Once
	initError error
}

func newDailyWriter(dir string) *dailyWriter {
	return &dailyWriter{dir: dir}
}

func (w *dailyWriter) Write(p []byte) (n int, err error) {
	w.initOnce.Do(func() {
		w.initError = os.MkdirAll(w.dir, 0755)
	})
	if w.initError != nil {
		return 0, w.initError
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	today := time.Now().Format("2006-01-02")
	if w.file == nil || w.curDate != today {
		if w.file != nil {
			w.file.Close()
		}
		filename := filepath.Join(w.dir, today+".log")
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return 0, err
		}
		w.file = file
		w.curDate = today
	}
	return w.file.Write(p)
}

func (w *dailyWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// LevelVar 可动态调整的日志级别
type LevelVar struct {
	level atomic.Int32
}

func (lv *LevelVar) Level() slog.Level {
	return slog.Level(lv.level.Load())
}

func (lv *LevelVar) Set(level slog.Level) {
	lv.level.Store(int32(level))
}

// Config 日志配置
type Config struct {
	Dir       string     // 日志目录
	Level     slog.Level // 初始日志级别
	AddSource bool       // 是否显示调用位置（仅对 text/json 有效）
	Format    string     // 输出格式: "simple"(默认) / "text" / "json"
}

// Logger 封装 slog.Logger
type Logger struct {
	*slog.Logger
	levelVar *LevelVar
	writer   *dailyWriter
}

// simpleHandler 自定义 Handler，输出格式: [HH:MM:SS][LEVEL] msg key=value ...
type simpleHandler struct {
	out   io.Writer
	level slog.Leveler
}

func (h *simpleHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *simpleHandler) Handle(_ context.Context, r slog.Record) error {
	// 时间格式: HH:MM:SS
	t := r.Time.Format("15:04:05")
	level := r.Level.String()
	msg := r.Message

	// 构建输出: [时间][级别] 消息
	buf := make([]byte, 0, 128)
	buf = append(buf, '[')
	buf = append(buf, t...)
	buf = append(buf, "]["...)
	buf = append(buf, level...)
	buf = append(buf, "] "...)
	buf = append(buf, msg...)

	// 追加所有属性 (key=value)
	if r.NumAttrs() > 0 {
		buf = append(buf, ' ')
		r.Attrs(func(a slog.Attr) bool {
			buf = append(buf, a.Key...)
			buf = append(buf, '=')
			buf = append(buf, fmt.Sprint(a.Value)...)
			buf = append(buf, ' ')
			return true
		})
		// 去掉最后一个空格
		if len(buf) > 0 && buf[len(buf)-1] == ' ' {
			buf = buf[:len(buf)-1]
		}
	}
	buf = append(buf, '\n')

	_, err := h.out.Write(buf)
	return err
}

func (h *simpleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// 简单实现：返回新 handler 但忽略附加属性（可扩展）
	return h
}

func (h *simpleHandler) WithGroup(name string) slog.Handler {
	// 忽略 group
	return h
}

// New 创建日志模块（懒加载：此时不创建目录/文件）
func New(cfg Config) (*Logger, error) {
	levelVar := &LevelVar{}
	levelVar.Set(cfg.Level)

	writer := newDailyWriter(cfg.Dir)
	multiWriter := io.MultiWriter(writer, os.Stdout)

	var handler slog.Handler
	switch cfg.Format {
	case "json":
		opts := &slog.HandlerOptions{
			Level:     levelVar,
			AddSource: cfg.AddSource,
		}
		handler = slog.NewJSONHandler(multiWriter, opts)
	case "text":
		opts := &slog.HandlerOptions{
			Level:     levelVar,
			AddSource: cfg.AddSource,
		}
		handler = slog.NewTextHandler(multiWriter, opts)
	default: // "simple" 或空
		handler = &simpleHandler{
			out:   multiWriter,
			level: levelVar,
		}
	}

	return &Logger{
		Logger:   slog.New(handler),
		levelVar: levelVar,
		writer:   writer,
	}, nil
}

// SetLevel 动态修改日志级别
func (l *Logger) SetLevel(level slog.Level) {
	l.levelVar.Set(level)
}

// Close 关闭日志文件
func (l *Logger) Close() error {
	return l.writer.Close()
}

// ----- 以下为新增的 Printf 风格方法（支持懒求值）-----

// Debugf 格式化 debug 日志，仅在级别启用时格式化
func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.Enabled(context.Background(), slog.LevelDebug) {
		l.Logger.Debug(fmt.Sprintf(format, args...))
	}
}

// Infof 格式化 info 日志
func (l *Logger) Infof(format string, args ...interface{}) {
	if l.Enabled(context.Background(), slog.LevelInfo) {
		l.Logger.Info(fmt.Sprintf(format, args...))
	}
}

// Warnf 格式化 warn 日志
func (l *Logger) Warnf(format string, args ...interface{}) {
	if l.Enabled(context.Background(), slog.LevelWarn) {
		l.Logger.Warn(fmt.Sprintf(format, args...))
	}
}

// Errorf 格式化 error 日志
func (l *Logger) Errorf(format string, args ...interface{}) {
	if l.Enabled(context.Background(), slog.LevelError) {
		l.Logger.Error(fmt.Sprintf(format, args...))
	}
}
