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

type FileLogger struct {
	path string
	z    *zap.Logger
}

var SysLogger *FileLogger

func init() {
	SysLogger = NewFileLog("")
}

func NewFileLog(p string) *FileLogger {
	var writer zapcore.WriteSyncer
	if p != "" {
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
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), writer, zapcore.InfoLevel)
	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.ErrorLevel), // Error 级别打印栈追踪
	)
	return &FileLogger{
		path: p,
		z:    logger,
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
