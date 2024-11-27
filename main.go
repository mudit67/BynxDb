package main

import (
	// _ "BynxDB/testing"
	"BynxDB/utils"
)

func main() {
	db, err := utils.TableInit()
	if err != nil {
		panic(err)
	}
	utils.ProcessQueries(db)
}

/*

func printRow(row []any) {
	// fmt.Print(row, ": ")
	for _, col := range row {
		switch data := col.(type) {
		case []byte:
			fmt.Print(string(data), " ")
		case int:
			fmt.Print(data, " ")
		}
	}
	fmt.Println()
}
*/
