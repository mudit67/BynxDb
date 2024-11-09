package main

import (
	"BynxDB/core"
	"fmt"
	"os"
)

func main() {
	tD := &core.TableDef{
		Types:      []uint16{core.TYPE_BYTE, core.TYPE_INT64, core.TYPE_BYTE, core.TYPE_BYTE},
		Cols:       []string{"Full_Name", "Emp_Id", "Dept", "email"},
		PKeyIndex:  1,
		UniqueCols: []int{1, 3},
	}

	db, err := core.DbInit("Employee", tD)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = db.Insert(1, []byte("Mudit Bhardwaj"), []byte("Software"), []byte("mudit@gmail.com"))
	if err != nil {
		fmt.Println(err)
	}
	err = db.Insert(2, []byte("Unnat Bhardwaj"), []byte("Software"), []byte("unnat@gmail.com"))
	if err != nil {
		fmt.Println(err)
	}
	err = db.Insert(3, []byte("Abhay Bhardwaj"), []byte("Software"), []byte("abhi@gmail.com"))
	if err != nil {
		fmt.Println(err)
	}
	err = db.Insert(4, []byte("Yash Bhardwaj"), []byte("Software"), []byte("yash@gmail.com"))
	if err != nil {
		fmt.Println(err)
	}

	// row, err := db.PKeyQuery(4)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// for _, cols := range row {
	// 	fmt.Println(string(cols.([]byte)))
	// }

	// row, err := db.PointQueryUniqueCol(3, []byte("yash@gmail.com"))
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// for _, cols := range row {
	// 	fmt.Println(string(cols.([]byte)))
	// }

	rows, err := db.PointQuery(2, []byte("Software"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Count: ", len(rows))
	for _, row := range rows {
		printRow(row)
	}
	db.Close()

	// col, _ := core.CollectionCreate([]byte("check"), &core.TableDef{})

	// _ = col.Put([]byte("Key1"), []byte("Value1"))
	// _ = col.Put([]byte("Key2"), []byte("Value2"))
	// _ = col.Put([]byte("Key3"), []byte("Unnat"))
	// _ = col.Put([]byte("Key4"), []byte("Mudit"))
	// _ = col.Put([]byte("Key5"), []byte("Value5"))
	// _ = col.Put([]byte("Key6"), []byte("Value6"))

	// col.Find([]byte("check"))
	// col.Find([]byte("Key4"))
	// it, _ := col.Find([]byte("Key3"))
	// fmt.Println(it)
	// col.Close()
}

func printRow(row []any) {
	for _, cols := range row {
		fmt.Print(string(cols.([]byte)), " ")
	}
	fmt.Println()
}
