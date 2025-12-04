package logs

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var z *zap.Logger

func init() {
	config := zap.NewProductionConfig()
	config.DisableCaller = false
	config.Encoding = "console"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	z, _ = config.Build()
}

func Debug(msg string, args ...interface{}) {
	z.Debug(fmt.Sprintf(msg, args...))
}

func Info(f string, args ...interface{}) {
	z.Info(fmt.Sprintf(f, args...))
}

func Warn(f string, args ...interface{}) {
	z.Warn(fmt.Sprintf(f, args...))
}

func Error(f string, args ...interface{}) {
	z.Error(fmt.Sprintf(f, args...))
}

func Errorf(f string, args ...interface{}) error {
	err := fmt.Errorf(f, args...)
	z.Error(err.Error())
	return err
}
