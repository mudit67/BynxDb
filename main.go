package main

import (
	"BynxDB/core"
	"fmt"
	"os"
)

func main() {
	dal, _ := core.DalCreate("./mainTest", os.Getpagesize())

	node, _ := dal.Getnode(dal.Root)
	node.DAL = dal

	index, containingNode, _ := node.Findkey([]byte("Key1"))

	res := containingNode.Items[index]

	fmt.Printf("Key is: %s, Value is: %s", res.Key, res.Value)

	dal.Close()
}
