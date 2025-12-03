package logs

import (
	"fmt"
	"go.uber.org/zap"
)

var z *zap.Logger

func init() {
	z, _ = zap.NewProduction()
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
