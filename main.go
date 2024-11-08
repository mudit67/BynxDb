package main

import (
	"BynxDB/core"
	"fmt"
	"os"
)

func main() {
	tD := &core.TableDef{Types: []uint16{core.TYPE_INT64, core.TYPE_BYTE}, Cols: []string{"ID", "Username"}}
	c, err := core.CollectionCreate([]byte("collection1"), tD)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// _ = c.Put([]byte("Key1"), []byte("Value1"))
	// _ = c.Put([]byte("Key2"), []byte("Value2"))
	// _ = c.Put([]byte("Key3"), []byte("Unnat"))
	// _ = c.Put([]byte("Key4"), []byte("Mudit"))
	// _ = c.Put([]byte("Key5"), []byte("Value5"))
	// _ = c.Put([]byte("Key6"), []byte("Value6"))
	// item, _ := c.Find([]byte("Key1"))

	// fmt.Printf("key is: %s, value is: %s\n", item.Key, item.Value)

	// _ = c.Remove([]byte("Key1"))
	// item, _ = c.Find([]byte("Key1"))

	// dal.Writefreelist()
	// fmt.Printf("item is: %+v\n", item)
	// _ = dal.Close()
	fmt.Println(string(c.Name), c.TableDef)
	c.Close()
}
