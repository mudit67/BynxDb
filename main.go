package main

import (
	"BynxDB/core"
	"BynxDB/core/utils"

	// _ "BynxDB/testing"
	"fmt"
)

func main() {
	utils.InitFileLogs()

	core.InitServer()
	// tD := &core.TableDef{
	// 	Cols:       []string{"ID", "Name", "Cabin", "Department_ID"},
	// 	Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE, core.TYPE_INT64, core.TYPE_INT64},
	// 	PKeyIndex:  0,
	// 	UniqueCols: []int{2},
	// }

	// db, err := core.DbInit("faculty", tD)
	// if err != nil {
	// 	utils.FatalError(err)
	// }

	// if rows, err := db.RangeQuery(1, []byte("A"), []byte("Z")); err != nil {
	// 	utils.Error(err)
	// } else {
	// 	fmt.Println("Result Row Count: ", len(rows))
	// 	for _, row := range rows {
	// 		printRow(row)
	// 	}
	// }

	// db.Close()
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
