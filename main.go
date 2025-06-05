package main

import (
	"BynxDB/core"
	"BynxDB/core/utils"

	// _ "BynxDB/testing"
	"fmt"
)

func main() {
	utils.InitFileLogs()
	tD := &core.TableDef{
		Cols:       []string{"ID", "Name", "Cabin", "Department_ID"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE, core.TYPE_INT64, core.TYPE_INT64},
		PKeyIndex:  0,
		UniqueCols: []int{2},
	}

	db, err := core.DbInit("faculty", tD)
	if err != nil {
		utils.FatalError(err)
	}

	db.Insert(60, []byte("Mudit Bhardwaj"), 200, 7)
	db.Insert(61, []byte("Kanishka Bhardwaj"), 201, 1)
	db.Insert(62, []byte("Yash Bhardwaj"), 202, 2)
	db.Insert(63, []byte("Unnat Bhardwaj"), 203, 2)
	db.Insert(64, []byte("Abhay Bhardwaj"), 204, 3)

	// rows, err := db.RangeQuery(2, 110, 1000)
	rows, err := db.SelectEntireTable()
	if err != nil {
		utils.Error(err)
	}
	fmt.Println("Result Row Count: ", len(rows))
	for _, row := range rows {
		printRow(row)
	}
	// db.PrintAllPages()
	db.Close()
}

func printRow(row []any) {
	// // fmt.print(row, ": ")
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
