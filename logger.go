package main

import (
	"github.com/loirsun/push_service/lg"
)

type Logger lg.Logger

const (
	LOG_DEBUG = lg.DEBUG
	LOG_INFO  = lg.INFO
	LOG_WARN  = lg.WARN
	LOG_ERROR = lg.ERROR
	LOG_FATAL = lg.FATAL
)

func (n *PushService) logf(level lg.LogLevel, f string, args ...interface{}) {
	opts := n.opts
	lg.Logf(opts.Logger, opts.logLevel, level, f, args...)
}
