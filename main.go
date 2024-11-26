package main

import (
	"BynxDB/core"
	_ "BynxDB/testing"
	"fmt"
	"os"
)

func main() {
	tD := &core.TableDef{
		Cols:       []string{"ID", "Name", "Cabin", "Department_ID"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE, core.TYPE_INT64, core.TYPE_INT64},
		PKeyIndex:  0,
		UniqueCols: []int{2},
	}

	db, err := core.DbInit("faculty", tD)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = db.Delete(3, 1)
	if err != nil {
		fmt.Println(err)
	}
	rows, err := db.RangeQuery(2, 104, 110)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Count: ", len(rows))
	for _, row := range rows {
		printRow(row)
	}
	db.Close()
}

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
