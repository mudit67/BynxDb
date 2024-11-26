package testing

import (
	"BynxDB/core"
	"encoding/json"
	"fmt"
	"os"
)

type table struct {
	Cols    []string `json:cols`
	Types   []uint16 `json:types`
	Unique  []int    `json:unique`
	Records [][]any  `json:records`
}

func init() {
	fmt.Println("Check")
	var table table
	data, err := os.ReadFile("./testing/faculty.json")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	json.Unmarshal(data, &table)
	fmt.Println(table.Cols)
	fmt.Println(table.Types)
	fmt.Println(table.Unique)
	fmt.Println(table.Records)
	tD := &core.TableDef{Cols: table.Cols, Types: table.Types, UniqueCols: table.Unique}
	db, err := core.DbInit("faculty", tD)
	if err != nil {
		fmt.Println(err)
	}
	for _, row := range table.Records {
		fmt.Println(row)
		for i, col := range row {
			switch data := col.(type) {
			case float64:
				row[i] = int(data)
			case string:
				row[i] = []byte(data)
			}
			fmt.Printf("%d: %T\n", i, row[i])
		}
		err := db.Insert(row...)
		if err != nil {
			panic(err)
		}
	}
	db.Close()
	fmt.Println("-- End Test --")
}
