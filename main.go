package main

import (
	"BynxDB/core"
	"fmt"
	"os"
)

func main() {
	tD := &core.TableDef{Types: []uint16{core.TYPE_BYTE, core.TYPE_INT64, core.TYPE_BYTE}, Cols: []string{"Full_Name", "Username", "Password"}, PKeyIndex: 1}

	db, err := core.DbInit("user", tD)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = db.Insert(
		1,
		[]byte("Mudit Bhardwaj"),
		[]byte("checkmate"))
	if err != nil {
		fmt.Println(err)
	}
	err = db.Insert(
		2,
		[]byte("Unnat Bhardwaj"),
		[]byte("checkmate"))
	if err != nil {
		fmt.Println(err)
	}
	err = db.Insert(
		3,
		[]byte("Abhi Bhardwaj"),
		[]byte("checkmate"))
	if err != nil {
		fmt.Println(err)
	}
	err = db.Insert(
		4,
		[]byte("Yash Bhardwaj"),
		[]byte("checkmate"))
	if err != nil {
		fmt.Println(err)
	}

	rows, err := db.PKeyQuery(3)
	if err != nil {
		fmt.Println(err)
	}
	for _, row := range rows {
		fmt.Println(string(row.([]byte)))
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
