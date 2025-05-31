/*
Package logger provides logging solution for ignite-go driver.

Log levels for the logger.Logger are:
  - OffLevel   : Do not log anything.
  - TraceLevel : The finest level, trace all cache operations. Do not use in production.
  - DebugLevel : A log message which can help with diagnosing a problem. Do not use in production.
  - InfoLevel  : An informational message.
  - WarnLevel  : A warning message.
  - ErrorLevel : An error message.

TraceLevel < DebugLevel < InfoLevel < WarnLevel < ErrorLevel < OffLevel.
If the sink's level is larger than a level of the logging message the sink must produce no output.

You can provide a custom logger sink that implements logger.Sink

	type Sink interface {
		Log(lvl Level, f func() string)
		Level() Level
	}

or use the default wrapper around log.Logger — create one using [logger.NewSink].
*/
package logger
