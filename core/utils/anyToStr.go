package utils

import "fmt"

func AnyToStr(arguments ...any) string {
	logString := ""
	for _, col := range arguments {
		switch data := col.(type) {
		case []byte:
			logString += (string(data) + " ")
		case int:
			logString += (fmt.Sprint(data) + " ")
		}
	}
	return logString
}
