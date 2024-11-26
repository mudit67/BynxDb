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

	rows, err := db.PointQuery(1, []byte("Henry Ford"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Count: ", len(rows))
	for _, row := range rows {
		printRow(row)
	}
	db.Close()
}

var data = [][]any{
	{1, []byte("Alice Johnson"), 101, 1},
	{2, []byte("Bob Smith"), 102, 2},
	{3, []byte("Charlie Brown"), 103, 1},
	{4, []byte("David Lee"), 104, 3},
	{5, []byte("Emily Wilson"), 105, 2},
	{6, []byte("Frank Miller"), 106, 1},
	{7, []byte("Grace Kelly"), 107, 3},
	{8, []byte("Henry Ford"), 108, 2},
	{9, []byte("Isabella Jones"), 109, 1},
	{10, []byte("Jack Daniels"), 110, 3},
	{11, []byte("Kate Winslet"), 111, 2},
	{12, []byte("Leo DiCaprio"), 112, 1},
	{13, []byte("Mia Wallace"), 113, 3},
	{14, []byte("Noah Centineo"), 114, 2},
	{15, []byte("Olivia Wilde"), 115, 1},
	{16, []byte("Peter Parker"), 116, 3},
	{17, []byte("Queen Elizabeth"), 117, 2},
	{18, []byte("Robert Downey Jr."), 118, 1},
	{19, []byte("Scarlett Johansson"), 119, 3},
	{20, []byte("Tom Cruise"), 120, 2},
}

func printRow(row []any) {
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
