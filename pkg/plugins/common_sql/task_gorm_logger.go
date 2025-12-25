package common_sql

import (
	"context"
	"fmt"
	"github.com/imiskolee/anycdc/pkg/core"
	"gorm.io/gorm/logger"
	"time"
)

type gormLogger struct {
	fileLogger *core.FileLogger
	level      logger.LogLevel
}

func (g gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	g.level = level
	return g
}

func (g gormLogger) Info(ctx context.Context, s string, i ...interface{}) {
	g.fileLogger.Debug(s, i...)
}

func (g gormLogger) Warn(ctx context.Context, s string, i ...interface{}) {
	g.fileLogger.Error(s, i...)
}

func (g gormLogger) Error(ctx context.Context, s string, i ...interface{}) {
	g.fileLogger.Error(s, i...)
}

func (g gormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	sql, rowsAffected := fc()
	g.fileLogger.Debug(fmt.Sprintf("%s [sql:%s][rowsAffected:%d]", begin, sql, rowsAffected))
}

func NewLogger(fileLogger *core.FileLogger) logger.Interface {
	return &gormLogger{
		fileLogger: fileLogger,
	}
}
