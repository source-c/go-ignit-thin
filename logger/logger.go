package logger

import (
	"fmt"
)

type Level int

const (
	TraceLevel Level = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	OffLevel

	_minLevel = TraceLevel
	_maxLevel = OffLevel
)

func (l Level) String() string {
	switch l {
	case TraceLevel:
		return "trace"
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	default:
		return "unknown"
	}
}

type Sink interface {
	Log(lvl Level, f func() string)
	Level() Level
}

type Logger struct {
	Sink
}

func (l Logger) Trace(f func() string) {
	l.Log(TraceLevel, f)
}

func (l Logger) Debug(f func() string) {
	l.Log(DebugLevel, f)
}

func (l Logger) Info(f func() string) {
	l.Log(InfoLevel, f)
}

func (l Logger) Infof(format string, values ...interface{}) {
	l.Log(InfoLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

func (l Logger) Warnf(format string, values ...interface{}) {
	l.Log(WarnLevel, func() string {
		return fmt.Sprintf(format, values...)
	})
}

func (l Logger) Errorf(format string, values ...interface{}) {
	l.Log(ErrorLevel, func() string {
		return fmt.Errorf(format, values...).Error()
	})
}
