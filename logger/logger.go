package logger

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path"
	"time"
)

type Logger struct {
	infoPath string
	debugPath string
	errorPath string
}

const (
	InfoPath = "info/"
	DebugPath = "debug/"
	ErrorPath = "error/"
)
func checkError(err error) {
	if err != nil {
		panic("Logger error: " + err.Error())
	}
}

func NewLogger(logPath string) *Logger {
	logger := &Logger{
		infoPath: path.Join(logPath, InfoPath),
		debugPath: path.Join(logPath, DebugPath),
		errorPath: path.Join(logPath, ErrorPath),
	}
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		checkError(os.Mkdir(logPath, os.ModePerm))
	}
	if _, err := os.Stat(logger.infoPath); os.IsNotExist(err) {
		checkError(os.Mkdir(logger.infoPath, os.ModePerm))
	}
	if _, err := os.Stat(logger.debugPath); os.IsNotExist(err) {
		checkError(os.Mkdir(logger.debugPath, os.ModePerm))
	}
	if _, err := os.Stat(logger.errorPath); os.IsNotExist(err) {
		checkError(os.Mkdir(logger.errorPath, os.ModePerm))
	}

	return logger
}

func (logger *Logger) Info(msg string) {
	appendFile(logger.infoPath, msg)
}
func (logger *Logger) Debug(msg string) {
	appendFile(logger.debugPath, msg)
}
func (logger *Logger) Error(msg string) {
	appendFile(logger.errorPath, msg)
}

func (logger *Logger) InfoJson(msg string, obj interface{}) {
	writeJson(logger.infoPath, msg, obj)
}
func (logger *Logger) DebugJson(msg string, obj interface{}) {
	writeJson(logger.debugPath, msg, obj)
}
func (logger *Logger) ErrorJson(msg string, obj interface{}) {
	writeJson(logger.errorPath, msg, obj)
}

func writeJson(fileName string, msg string, obj interface{}) {
	byteObj, _ := json.Marshal(obj)
	appendFile(fileName, msg + " " + string(byteObj))
}

func appendFile(filePath string, msg string) {
	time := time.Now()
	timeStr := time.Format("2006-01-02")
	f, err := os.OpenFile(path.Join(filePath, timeStr + ".log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Logger error: " + err.Error())
	}
	defer f.Close()
	wrt := io.MultiWriter(os.Stdout, f)
	log.SetOutput(wrt)
	log.Printf(": %s\n", msg)
}