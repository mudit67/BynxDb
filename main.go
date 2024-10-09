package main

import (
	"BynxDB/core"
	"os"
)

func main() {
	dal, _ := core.DalCreate("check.db", os.Getpagesize())

	p := dal.AllocateEmptyPage()
	p.Num = dal.GetNextPage()
	copy(p.Data, "Data")

	_ = dal.WritePage(p)
	_, _ = dal.WriteFreelist()

	_ = dal.Close()

	dal, _ = core.DalCreate("check.db", os.Getpagesize())

	p = dal.AllocateEmptyPage()
	p.Num = dal.GetNextPage()
	copy(p.Data, "Data 2")
	_ = dal.WritePage(p)

	pageNum := dal.GetNextPage()
	dal.ReleasedPage(pageNum)

	_, _ = dal.WriteFreelist()

}
