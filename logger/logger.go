package logger

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

type Logger struct {
	inner  *logrus.Entry
	prefix string
}

func NewLogger(ctx context.Context) *Logger {
	inner := logrus.New()
	return &Logger{
		inner:  inner.WithFields(logrus.Fields{}),
		prefix: "",
	}
}

func (l *Logger) SetLevel(lvlStr string) *Logger {
	lvl, err := logrus.ParseLevel(lvlStr)
	if err != nil {
		panic(fmt.Sprintf("failed to parse logger level %s: %v", lvlStr, err))
	}
	l.inner.Logger.SetLevel(lvl)
	return l
}

func (l *Logger) SetPrefix(prefix string) *Logger {
	l.prefix = prefix
	return l
}

func (l *Logger) Field(key string, val string) *Logger {
	return &Logger{
		inner:  l.inner.WithField(key, val),
		prefix: l.prefix,
	}
}

func (l *Logger) Fieldf(key string, format string, args ...interface{}) *Logger {
	val := fmt.Sprintf(format, args...)
	return l.Field(key, val)
}

func (l *Logger) Fatalf(ctx context.Context, format string, args ...interface{}) {
	l.inner.Fatalf(format, args...)
}

func (l *Logger) Errorf(ctx context.Context, format string, args ...interface{}) *Logger {
	l.inner.Errorf(format, args...)
	return l
}

func (l *Logger) Warnf(ctx context.Context, format string, args ...interface{}) *Logger {
	l.inner.Warnf(format, args...)
	return l
}

func (l *Logger) Infof(ctx context.Context, format string, args ...interface{}) *Logger {
	l.inner.Infof(format, args...)
	return l
}

func (l *Logger) Debugf(ctx context.Context, format string, args ...interface{}) *Logger {
	l.inner.Debugf(format, args...)
	return l
}

func (l *Logger) Tracef(ctx context.Context, format string, args ...interface{}) *Logger {
	l.inner.Tracef(format, args...)
	return l
}
