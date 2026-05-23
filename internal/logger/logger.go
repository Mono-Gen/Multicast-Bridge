package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func ParseLevel(lvl string) Level {
	switch strings.ToUpper(lvl) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO
	}
}

type Logger struct {
	mu         sync.Mutex
	level      Level
	filePath   string
	file       *os.File
	maxSize    int64 // Maximum file size (5MB default)
	maxBackups int   // Maximum number of backups (3 default)
}

var (
	globalLogger *Logger
	once         sync.Once
)

// GetLogger returns the global logger instance.
func GetLogger() *Logger {
	once.Do(func() {
		globalLogger = &Logger{
			level:      INFO,
			maxSize:    5 * 1024 * 1024, // 5MB
			maxBackups: 3,
		}
	})
	return globalLogger
}

// Init initializes the global logger with a specific level and file path.
func Init(levelStr string, filePath string) error {
	l := GetLogger()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.level = ParseLevel(levelStr)
	l.filePath = filePath

	if l.file != nil {
		l.file.Close()
		l.file = nil
	}

	if filePath != "" {
		// Ensure directory exists
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %w", err)
		}

		// Open file
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		l.file = f
	}

	return nil
}

// Close closes the log file.
func Close() {
	l := GetLogger()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		l.file.Sync()
		l.file.Close()
		l.file = nil
	}
}

// Log writes a log message.
func (l *Logger) Log(level Level, code int, format string, v ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, v...)

	var logLine string
	if code > 0 {
		logLine = fmt.Sprintf("[%s] [%s] [%d] %s\n", now, level.String(), code, msg)
	} else {
		logLine = fmt.Sprintf("[%s] [%s] %s\n", now, level.String(), msg)
	}

	// Write to console
	fmt.Print(logLine)

	// Write to file if enabled
	if l.file != nil {
		l.checkRotation()
		l.file.WriteString(logLine)
	}
}

// checkRotation performs simple log rotation if the file exceeds maxSize.
func (l *Logger) checkRotation() {
	if l.file == nil || l.filePath == "" {
		return
	}

	info, err := l.file.Stat()
	if err != nil {
		return
	}

	if info.Size() < l.maxSize {
		return
	}

	// Close current file
	l.file.Sync()
	l.file.Close()

	// Rotate backups
	for i := l.maxBackups - 1; i >= 1; i-- {
		oldPath := fmt.Sprintf("%s.%d", l.filePath, i)
		newPath := fmt.Sprintf("%s.%d", l.filePath, i+1)
		if _, err := os.Stat(oldPath); err == nil {
			os.Rename(oldPath, newPath)
		}
	}

	// Rename current file to file.1
	backupPath := fmt.Sprintf("%s.1", l.filePath)
	os.Rename(l.filePath, backupPath)

	// Open new file
	f, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		l.file = f
	}
}

// Helper methods for log level logging
func Debugf(format string, v ...interface{}) {
	GetLogger().Log(DEBUG, 0, format, v...)
}

func Infof(format string, v ...interface{}) {
	GetLogger().Log(INFO, 0, format, v...)
}

func Warnf(code int, format string, v ...interface{}) {
	GetLogger().Log(WARN, code, format, v...)
}

func Errorf(code int, format string, v ...interface{}) {
	GetLogger().Log(ERROR, code, format, v...)
}
