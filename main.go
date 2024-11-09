package main

import (
	"BynxDB/core"
	"fmt"
	"os"
)

func main() {
	tD := &core.TableDef{Types: []uint16{core.TYPE_BYTE, core.TYPE_BYTE, core.TYPE_BYTE}, Cols: []string{"Full_Name", "Username", "Password"}, PKeyIndex: 1}

	db, err := core.DbInit("user", tD)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	err = db.Insert(
		[]byte("mudit67"),
		[]byte("Mudit Bhardwaj"),
		[]byte("checkmate"))
	if err != nil {
		fmt.Println(err)
	}
	rows, err := db.PKeyQuery([]byte("mudit67"))
	if err != nil {
		fmt.Println(err)
	}
	for _, row := range rows {
		fmt.Println(string(row.([]byte)))
	}
	db.Close()
}
