package logger

import (
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func New(level string) *logrus.Entry {
	return NewWithWriter(level, os.Stderr)
}

func NewWithWriter(level string, writer io.Writer) *logrus.Entry {
	log := &logrus.Logger{
		Out:       writer,
		Formatter: new(logrus.JSONFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.InfoLevel,
	}

	if logrusLevel, err := logrus.ParseLevel(level); err != nil {
		fmt.Println(errors.Wrap(err, "using 'info' as default").Error())
	} else {
		log.Level = logrusLevel
	}

	return logrus.NewEntry(log)
}
