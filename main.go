package main

import (
	"BynxDB/core"
	"os"
)

func main() {
	dal, _ := core.DalCreate("check.db", os.Getpagesize())

	// Creating a page, writing a data draft let's say
	p := dal.AllocateEmptyPage()
	p.Num = dal.GetNextPage()
	copy(p.Data, "data")
	// commiting the data
	_ = dal.WritePage(p)
}
