package core

import (
	"BynxDB/core/utils"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

type Collection struct {
	Name []byte
	// root pgNum
	DAL *DAL
	*TableDef
}

var options = &Options{
	PageSize:       os.Getpagesize(),
	MinFillPercent: 0.0125,
	MaxFillPercent: 0.025,
}

// getProjectRoot finds the project root directory by looking for go.mod file
func getProjectRoot() (string, error) {
	// Get the path of this source file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("unable to get current file path")
	}

	// Start from the directory containing this file
	dir := filepath.Dir(filename)

	// Walk up the directory tree until we find go.mod
	for {
		goModPath := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			// Found go.mod, this is the project root
			return dir, nil
		}

		// Move up one directory
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached the root of the filesystem without finding go.mod
			return "", fmt.Errorf("could not find project root (go.mod not found)")
		}
		dir = parent
	}
}

func CollectionCreate(name []byte, tD *TableDef) (*Collection, error) {
	utils.Info(1, "Init "+string(name)+" Collections.")
	c := &Collection{
		Name:     name,
		TableDef: tD,
	}
	rootDir, err := getProjectRoot()
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(rootDir, "db", string(name)+".db")
	dal, err := DalCreate(dbPath, options)
	if err != nil {
		// fmt.println(err)
		os.Exit(1)
	}
	c.DAL = dal
	if c.DAL.TableDefPage != 0 {
		utils.Info(1, "Old table def: ", c.DAL.TableDefPage)
		tableDefPage, err := c.DAL.Readpage(c.DAL.TableDefPage)
		if err != nil {
			// fmt.println("Error in reading tableDef")
			return nil, err
		}
		c.TableDef = &TableDef{}
		c.TableDef.Deserialize(tableDefPage.Data)
	} else {
		utils.Info(1, "Creating new TableDef")
		for i := range tD.UniqueCols {
			if tD.UniqueCols[i] == tD.PKeyIndex {
				tD.UniqueCols = append(tD.UniqueCols[:i], tD.UniqueCols[i+1:]...)
				break
			}
		}
		if tD.PKeyIndex != 0 {
			tD.Cols[tD.PKeyIndex], tD.Cols[0] = tD.Cols[0], tD.Cols[tD.PKeyIndex]
			tD.Types[tD.PKeyIndex], tD.Types[0] = tD.Types[0], tD.Types[tD.PKeyIndex]
		}
		tableDefPage := c.DAL.Allocateemptypage()
		tableDefPage.Num = c.DAL.GetNextPage()
		tableDefPage.Data = c.TableDef.Serialize(tableDefPage.Data)
		c.DAL.TableDefPage = tableDefPage.Num
		utils.Info(1, "TableDefPage: ", tableDefPage.Num)

		c.DAL.Writepage(tableDefPage)
		tD.PKeyIndex = 0
		if c.DAL.Root == 0 {
			c.DAL.Root = c.DAL.GetNextPage()
		}
		utils.Info(1, "Collection: new Root Page: ", c.DAL.Root)

		rootPage := c.DAL.Allocateemptypage()
		rootPage.Num = c.DAL.Root
		c.DAL.Writepage(rootPage)

		rootNode := &Node{}
		rootNode.Pagenum = c.DAL.Root
		c.DAL.Writenode(rootNode)
		c.DAL.Writemeta(c.DAL.Meta)
	}

	return c, nil
}

func (c *Collection) Close() {
	utils.Info(1, "Closing ", string(c.Name), "Collection")
	c.DAL.Writemeta(c.DAL.Meta)
	c.DAL.Writefreelist()
	c.DAL.Close()
}

// TODO: Add ancestorsIndexs

func (c *Collection) Find(key []byte) (*Item, error) {

	// // fmt.println("Search for Key: ", key)
	root, err := c.DAL.Getnode(c.DAL.Root)
	if err != nil {
		return nil, err
	}
	index, containingNode, _, err := root.Findkey(key, true)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, nil
	}
	return containingNode.Items[index], nil
}

func (c *Collection) FetchAll(pageNum pgNum) ([]*Item, error) {
	items := []*Item{}
	if pageNum == 0 {
		pageNum = c.DAL.Root
	}
	node, err := c.DAL.Getnode(pageNum)
	if err != nil {
		return nil, err
	}
	items = append(items, node.Items...)
	if !node.Isleaf() {
		for _, childNode := range node.Childnodes {
			subItems, err := c.FetchAll(childNode)
			if err != nil {
				return nil, err
			}
			items = append(items, subItems...)
		}
	}
	return items, nil
}

func (c *Collection) FindInBetween(low []byte, high []byte) ([]*Item, error) {
	// fmt.println("-- Range query --")
	root, err := c.DAL.Getnode(c.DAL.Root)
	if err != nil {
		return nil, err
	}

	items := []*Item{}

	lowIndex, containingNode, _, err := root.Findkey(low, false)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(containingNode.Items[lowIndex].Key, low) {
		items = append(items, containingNode.Items[lowIndex])
	}
	sameNode, highIndex := containingNode.Findkeyinnode(high)
	// Case 1:  High key is in the same node
	if sameNode {
		items = append(items, containingNode.Items[highIndex])
		// fmt.println(containingNode.Items[lowIndex])
	}
	// fmt.println(lowIndex, containingNode, containingNode.Pagenum)
	// for _, it := range containingNode.Items {
	// fmt.println(it.Key)
	// }
	// // fmt.println(sameNode, containingNode.Items[0].Key, containingNode.Items[1].Key)

	return items, nil
}

func (c *Collection) Put(key []byte, value []byte, update bool) error {
	utils.Info(2, "Collection Put Call", "Update:", update)
	i := ItemCreate(key, value)

	var root *Node
	var err error

	// if c.DAL.Root == 0 {
	// 	if update {
	// 		return errors.New("[error] no data in the table")
	// 	}
	// 	// fmt.println("Creating new root")
	// 	nodeTemp := c.DAL.nodeCreate([]*Item{i}, []pgNum{})
	// 	root, err = c.DAL.Writenode(nodeTemp)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	c.DAL.Root = root.Pagenum
	// 	c.DAL.Meta.Root = root.Pagenum
	// 	c.DAL.Writemeta(c.DAL.Meta)
	// 	return nil
	// }
	root, err = c.DAL.Getnode(c.DAL.Root)
	if err != nil {
		return err
	}
	rootString := c.nodeState(root)
	utils.Info(2, "Root Node: ", rootString)
	insertionIndex, nodeToInsertIn, ancestorsIndexes, err := root.Findkey(i.Key, false)
	if err != nil {
		return err
	}

	if update && (nodeToInsertIn == nil) {
		return errors.New("[error] row not found")
	}
	utils.Info(2, "nodeToInsertIn: ", c.nodeState(nodeToInsertIn))
	if nodeToInsertIn.Items != nil && insertionIndex < len(nodeToInsertIn.Items) && bytes.Equal(nodeToInsertIn.Items[insertionIndex].Key, key) {
		if !update {
			utils.Error("Key Already Exists")
			return errors.New("[error] this key already excists in the key-value store")
		}
		utils.Info(2, "Updating Item at: ", insertionIndex)
		nodeToInsertIn.Items[insertionIndex] = i
	} else {
		utils.Info(2, "Inserting Item at: ", insertionIndex)
		nodeToInsertIn.addItem(i, insertionIndex)
	}
	utils.Info(3, "Writing NodeToInsert: ", c.nodeState(nodeToInsertIn))
	_, err = c.DAL.Writenode(nodeToInsertIn)
	if err != nil {
		return nil
	}
	ancestors, err := c.GetNodes(ancestorsIndexes)
	if err != nil {
		return err
	}

	// * Handle rebalancing
	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]
		utils.Info(3, "Check: ", c.nodeState(node))
		if node.isOverPopulated() {
			utils.Info(2, "Calling split on: ", len(node.Items))
			pnode.split(node, nodeIndex)
		}
	}

	rootNode := ancestors[0]
	utils.Info(3, "Checking Root: ", c.nodeState(rootNode))
	if rootNode.isOverPopulated() {
		newNode := c.DAL.nodeCreate([]*Item{}, []pgNum{rootNode.Pagenum})
		utils.Info(2, "Calling split on: ", len(rootNode.Items))
		newNode.split(rootNode, 0)
		newRoot, err := c.DAL.Writenode(newNode)
		if err != nil {
			return err
		}
		c.DAL.Root = newRoot.Pagenum
		c.DAL.Meta.Root = newRoot.Pagenum
		c.DAL.Writemeta(c.DAL.Meta)
	}
	return nil
}

func (c *Collection) GetNodes(indexes []int) ([]*Node, error) {
	root, err := c.DAL.Getnode(c.DAL.Root)
	if err != nil {
		return nil, err
	}
	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err = c.DAL.Getnode(child.Childnodes[indexes[i]])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}

// * Remove removes a key from the tree. It finds the correct node and the index to remove the item from and removes it.
// * When performing the search, the ancestors are returned as well. This way we can iterate over them to check which
func (c *Collection) Remove(key []byte) error {
	// * nodes were modified and rebalance by rotating or merging the unbalanced nodes. Rotation is done first. If the
	// * siblings don't have enough items, then merging occurs. If the root is without items after a split, then the root is
	// * removed and the tree is one level shorter.
	rootNode, err := c.DAL.Getnode(c.DAL.Root)
	if err != nil {
		return nil
	}

	removeItemIndex, nodeToRemoveFrom, anscestorIndexes, err := rootNode.Findkey(key, true)

	if err != nil || removeItemIndex == -1 {
		return err
	}

	if nodeToRemoveFrom.Isleaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex)
	} else {
		affectedNodes, err := nodeToRemoveFrom.removeItemFromInternal(removeItemIndex)
		if err != nil {
			return err
		}
		anscestorIndexes = append(anscestorIndexes, affectedNodes...)
	}

	ancestors, err := c.GetNodes(anscestorIndexes)
	if err != nil {
		return err
	}
	// * Rebalance the nodes all the way up. Start From one node before the last and go all the way up. Exclude root.
	for i := len(ancestors) - 2; i >= 0; i-- {
		pNode := ancestors[i]
		aNode := ancestors[i+1]
		if aNode.isUnderPopulated() {
			err := pNode.rebalanceRemove(aNode, anscestorIndexes[i+1])
			if err != nil {
				return err
			}
		}
	}

	rootNode = ancestors[0]
	// * If the root has no items after rebalancing, there's no need to save it because we ignore it.
	if len(rootNode.Items) == 0 && len(rootNode.Childnodes) > 0 {
		c.DAL.Root = ancestors[1].Pagenum
	}

	return nil
}

func (c *Collection) nodeState(node *Node) string {

	logString := ""
	for _, item := range node.Items {
		row := decodeRow(c.TableDef, item.Value)
		itKey, _ := checkTypeAndDecodeCol(c.TableDef, 0, item.Key)
		row = append([]any{itKey}, row...)
		logString += utils.AnyToStr(row...)
	}
	if len(node.Childnodes) != 0 {
		logString += " ChildNodes: "
		for _, pgn := range node.Childnodes {
			logString += " " + fmt.Sprint(pgn)
		}

	} else {
		logString += "--Leaf Node--"
	}
	return logString
}

func (c *Collection) PrintAllRecords() {
	utils.SLog("==Reading All Pages: "+string(c.Name), fmt.Sprint(c.DAL.Root)+":"+fmt.Sprint(c.DAL.maxPage)+" ==")
	for i := c.DAL.TableDefPage + 1; i < c.DAL.maxPage; i++ {
		node, err := c.DAL.Getnode(i)
		if err != nil {
			utils.Error(err)
			return
		}
		logString := c.nodeState(node)
		utils.SLog(i, logString)

	}
}
