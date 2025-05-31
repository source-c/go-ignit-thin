package logger

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"log"
	"strings"
	"testing"
)

func TestDefaultSink_LevelsFilter(t *testing.T) {
	for loggerLvl := _minLevel; loggerLvl <= _maxLevel; loggerLvl++ {
		buf := bytes.Buffer{}
		sink, _ := NewSink(log.New(&buf, "", log.LstdFlags), loggerLvl)
		testLog := &Logger{sink}
		for curLvl := _minLevel; curLvl <= _maxLevel; curLvl++ {
			t.Run(fmt.Sprintf("loggerLevel: %s, curLevel %s", loggerLvl, curLvl), func(t *testing.T) {
				defer func() {
					buf.Reset()
				}()
				testLog.Log(curLvl, func() string {
					return "test"
				})
				require.Equal(t, loggerLvl, testLog.Level())
				contains := strings.Contains(buf.String(), strings.ToUpper(curLvl.String()))
				if curLvl < testLog.Level() {
					require.False(t, contains, fmt.Sprintf("unexpected output %s for level %s", buf.String(), testLog.Level()))
				} else {
					require.True(t, contains, fmt.Sprintf("unexpected output %s for level %s", buf.String(), testLog.Level()))
				}
			})
		}
	}
}

func TestDefaultSink_Basic(t *testing.T) {
	buf := bytes.Buffer{}
	sink, _ := NewSink(log.New(&buf, "", log.LstdFlags), _minLevel)
	testLog := &Logger{sink}

	fixtures := []struct {
		name     string
		run      func(logger *Logger)
		patterns []string
	}{
		{
			"Trace",
			func(logger *Logger) {
				logger.Trace(func() string {
					return "test"
				})
			},
			[]string{strings.ToUpper(TraceLevel.String()), "test"},
		},
		{
			"Debug",
			func(logger *Logger) {
				logger.Debug(func() string {
					return "test"
				})
			},
			[]string{strings.ToUpper(DebugLevel.String()), "test"},
		},
		{
			"Info",
			func(logger *Logger) {
				logger.Info(func() string {
					return "test"
				})
			},
			[]string{strings.ToUpper(InfoLevel.String()), "test"},
		},
		{
			"Infof",
			func(logger *Logger) {
				logger.Infof("%s", "test")
			},
			[]string{strings.ToUpper(InfoLevel.String()), "test"},
		},
		{
			"Warnf",
			func(logger *Logger) {
				logger.Warnf("%s", "test")
			},
			[]string{strings.ToUpper(WarnLevel.String()), "test"},
		},
		{
			"Errorf",
			func(logger *Logger) {
				logger.Errorf("%s", "test")
			},
			[]string{strings.ToUpper(ErrorLevel.String()), "test"},
		},
	}

	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			defer func() {
				buf.Reset()
			}()
			f.run(testLog)
			for _, pattern := range f.patterns {
				require.True(t, strings.Contains(buf.String(), pattern))
			}
		})
	}
}

func TestDefaultSinkCreationError(t *testing.T) {
	_, err := NewSink(nil, _minLevel-1)
	require.Error(t, err)
	_, err = NewSink(nil, _maxLevel+1)
	require.Error(t, err)
}
