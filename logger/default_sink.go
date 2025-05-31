package logger

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

type defaultSink struct {
	l       *log.Logger
	sinkLvl Level
}

func NewSink(l *log.Logger, lvl Level) (Sink, error) {
	if lvl < _minLevel || lvl > _maxLevel {
		return nil, errors.New("invalid level")
	}
	return &defaultSink{l, lvl}, nil
}

func (sink *defaultSink) Log(lvl Level, f func() string) {
	if lvl < sink.sinkLvl || sink.l == nil {
		return
	}
	_ = sink.l.Output(2, fmt.Sprintf("%-5s: %s", strings.ToUpper(lvl.String()), f()))
}

func (sink *defaultSink) Level() Level {
	return sink.sinkLvl
}
