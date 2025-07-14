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

	db.Insert(10, []byte("Mudit Bhardwaj"), 1000, 3)
	db.Insert(11, []byte("Unnat Bhardwaj"), 1001, 3)
	db.Insert(12, []byte("Check Bhardwaj"), 1002, 3)

	// db.Delete(0, 10)

	rows, _ := db.RangeQuery(2, 1000, 1001)

	printRows(rows)

	db.Close()
}

func printRows(rows [][]any) {
	for _, row := range rows {
		printRow(row)
	}
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
