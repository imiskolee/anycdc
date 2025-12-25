package core

import (
	"errors"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"path"
	"path/filepath"
)

const (
	maxSize    = 1024 * 1024 * 10 //50mb
	LevelDebug = "debug"
	LevelInfo  = "info"
)

func toZapLevel(level string) zapcore.Level {
	switch level {
	case LevelDebug:
		return zapcore.DebugLevel
	case LevelInfo:
		return zapcore.InfoLevel
	}
	return zapcore.InfoLevel
}

type FileLogger struct {
	path    string
	rawFile *os.File
	z       *zap.Logger
}

var SysLogger *FileLogger

func init() {
	SysLogger = NewFileLog("", LevelInfo)
}

func NewFileLog(p string, level string) *FileLogger {
	var writer zapcore.WriteSyncer
	var raw *os.File
	if p != "" && !config.G.Tester {
		fullPath := path.Join(config.G.DataDir, p)
		dir := filepath.Dir(fullPath)
		// Step 3: Create directory (and parents) if it doesn't exist
		if err := os.MkdirAll(dir, 0644); err != nil {
			panic(err)
		}
		logFile, err := os.OpenFile(
			fullPath,                            // 日志文件路径
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, // 模式：创建+写入+追加
			0644,                                // 文件权限
		)
		if err != nil {
			panic("创建日志文件失败: " + err.Error())
		}
		raw = logFile
		writer = zapcore.Lock(logFile)
	} else {
		writer = os.Stdout
	}
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:      "time",
		LevelKey:     "level",
		CallerKey:    "caller",
		MessageKey:   "msg",
		LineEnding:   zapcore.DefaultLineEnding,
		EncodeLevel:  zapcore.CapitalLevelEncoder,                        // 带颜色的级别（控制台友好）
		EncodeTime:   zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05"), // 自定义时间格式
		EncodeCaller: zapcore.ShortCallerEncoder,
	}
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), writer, toZapLevel(level))
	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.ErrorLevel), // Error 级别打印栈追踪
	)
	return &FileLogger{
		path:    p,
		rawFile: raw,
		z:       logger,
	}
}

func (s *FileLogger) Debug(msg string, args ...interface{}) {
	s.z.Debug(fmt.Sprintf(msg, args...))
}

func (s *FileLogger) Info(msg string, args ...interface{}) {
	s.z.Info(fmt.Sprintf(msg, args...))
}
func (s *FileLogger) Error(msg string, args ...interface{}) {
	s.z.Error(fmt.Sprintf(msg, args...))
	if s.path != "" {
		SysLogger.Error(msg, args...)
	}
}

func (s *FileLogger) Errorf(msg string, args ...interface{}) error {
	l := fmt.Sprintf(msg, args...)
	s.z.Error(l)
	return errors.New(l)
}

func (s *FileLogger) Rotate() {
	if s.rawFile == nil {
		return
	}
	fileStat, err := os.Stat(s.path)
	if err != nil {
		SysLogger.Error("can not rotate log %s", s.path)
		return
	}
	size := fileStat.Size()
	if size > maxSize {
		_ = s.rawFile.Truncate(maxSize / 2)
	}
}
