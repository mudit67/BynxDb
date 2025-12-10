package testing

import (
	"BynxDB/core"
	"encoding/json"
	"os"
)

type table struct {
	Cols    []string `json:"cols"`
	Types   []uint16 `json:"types"`
	Unique  []int    `json:"unique"`
	Records [][]any  `json:"records"`
}

func init() {
	// fmt.println("Check")
	var table table
	data, err := os.ReadFile("./testing/faculty.json")
	if err != nil {
		// fmt.println(err)
		os.Exit(1)
	}
	json.Unmarshal(data, &table)
	// fmt.println(table.Cols)
	// fmt.println(table.Types)
	// fmt.println(table.Unique)
	// fmt.println(table.Records)
	tD := &core.TableDef{Cols: table.Cols, Types: table.Types, UniqueCols: table.Unique}
	db, err := core.DbInit("faculty", tD)
	if err != nil {
		// fmt.println(err)
	}
	for _, row := range table.Records {
		// fmt.println(row)
		for i, col := range row {
			switch data := col.(type) {
			case float64:
				row[i] = int(data)
			case string:
				row[i] = []byte(data)
			}
			// fmt.printf("%d: %T\n", i, row[i])
		}
		err := db.Insert(row...)
		if err != nil {
			// panic(err)
			// fmt.println(err)
		}
	}
	db.Close()
	// fmt.println("-- End Test --")
}
