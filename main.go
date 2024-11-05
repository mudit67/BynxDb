package main

import (
	"BynxDB/core"
	"fmt"
	"os"
)

func main() {
	options := &core.Options{
		PageSize:       os.Getpagesize(),
		MinFillPercent: 0.0125,
		MaxFillPercent: 0.025,
	}
	dal, _ := core.DalCreate("./check.db", options)

	c := core.CollectionCreate([]byte("collection1"), dal.Root)
	c.DAL = dal

	_ = c.Put([]byte("Key1"), []byte("Value1"))
	_ = c.Put([]byte("Key2"), []byte("Value2"))
	_ = c.Put([]byte("Key3"), []byte("Unnat"))
	_ = c.Put([]byte("Key4"), []byte("Mudit"))
	_ = c.Put([]byte("Key5"), []byte("Value5"))
	_ = c.Put([]byte("Key6"), []byte("Value6"))
	item, _ := c.Find([]byte("Key1"))

	fmt.Printf("key is: %s, value is: %s\n", item.Key, item.Value)

	_ = c.Remove([]byte("Key1"))
	item, _ = c.Find([]byte("Key1"))

	dal.Writefreelist()
	fmt.Printf("item is: %+v\n", item)
	_ = dal.Close()
}
