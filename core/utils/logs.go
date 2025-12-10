package utils

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

const logsFilePath = "logs/app.log"

var logDepth int = int(^uint(0) >> 1)

func InitFileLogs() {
	if len(os.Args) > 1 {
		fmt.Println(os.Args[1])
		logDepth, _ = strconv.Atoi(os.Args[1])
	}
	// Ensure the logs directory exists
	logsDir := "logs"
	if _, err := os.Stat(logsDir); os.IsNotExist(err) {
		err := os.MkdirAll(logsDir, 0755)
		if err != nil {
			log.Fatalln("Unable to create Logs Directory")
		}
	}
	file, err := os.OpenFile(logsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("Unable to load Logs File")
	}
	log.SetOutput(file)
	log.Println("Logs Init with Depth: ", logDepth)
}

func Info(priority int, msg ...any) {
	if priority <= logDepth {
		log.Println("INFO:", msg)
	}
}
func InfoLogAndPrint(msg ...any) {
	fmt.Println("INFO:", msg)
	log.Println("INFO:", msg)
}

func Error(msg ...any) {
	fmt.Println("ERROR:", msg)
	log.Println("ERROR:", msg)
}

func Warn(msg ...any) {
	log.Println("WARN:", msg)
}
func FatalError(msg ...any) {
	fmt.Println("ERROR:", msg)
	log.Fatalln("ERROR:", msg)
}

func SLog(msg ...any) {
	if logDepth > 0 {
		log.Println(msg...)
	}
}

func SLognln(msg ...any) {
	if logDepth > 0 {
		log.Print(msg...)
	}
}
