package core

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

type Collection struct {
	Name []byte
	root pgNum
	DAL  *DAL
	*TableDef
}

var options = &Options{
	PageSize:       os.Getpagesize(),
	MinFillPercent: 0.0125,
	MaxFillPercent: 0.025,
}

func CollectionCreate(name []byte, tD *TableDef) (*Collection, error) {
	c := &Collection{
		Name:     name,
		TableDef: tD,
	}
	dal, err := DalCreate("./db/"+string(name)+".db", options)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	c.DAL = dal
	if c.DAL.TableDefPage != 0 {
		fmt.Println("Old table def: ", c.DAL.TableDefPage)
		tableDefPage, err := c.DAL.Readpage(c.DAL.TableDefPage)
		if err != nil {
			fmt.Println("Error in reading tableDef")
			return nil, err
		}
		c.TableDef = &TableDef{}
		c.TableDef.Deserialize(tableDefPage.Data)
	} else {
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
		fmt.Println(tableDefPage.Num)

		c.DAL.Writepage(tableDefPage)
		c.DAL.Writemeta(c.DAL.Meta)
	}
	tD.PKeyIndex = 0
	c.root = c.DAL.Root
	fmt.Println(c.TableDef)
	return c, nil
}

func (c *Collection) Close() {
	c.DAL.Writemeta(c.DAL.Meta)
	c.DAL.Writefreelist()
	c.DAL.Close()
}

// TODO: Add ancestorsIndexs

func (c *Collection) Find(key []byte) (*Item, error) {

	// fmt.Println("Search for Key: ", key)
	root, err := c.DAL.Getnode(c.root)
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
		pageNum = c.root
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
	fmt.Println("-- Range query --")
	root, err := c.DAL.Getnode(c.root)
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
		fmt.Println(containingNode.Items[lowIndex])
	}
	fmt.Println(lowIndex, containingNode, containingNode.Pagenum)
	for _, it := range containingNode.Items {
		fmt.Println(it.Key)
	}
	// fmt.Println(sameNode, containingNode.Items[0].Key, containingNode.Items[1].Key)

	return items, nil
}

func (c *Collection) Put(key []byte, value []byte) error {
	i := ItemCreate(key, value)

	var root *Node
	var err error

	if c.root == 0 {
		fmt.Println("Creating new root")
		nodeTemp := c.DAL.nodeCreate([]*Item{i}, []pgNum{})
		root, err = c.DAL.Writenode(nodeTemp)
		if err != nil {
			return err
		}
		c.root = root.Pagenum
		c.DAL.Meta.Root = root.Pagenum
		c.DAL.Writemeta(c.DAL.Meta)
		return nil
	}
	root, err = c.DAL.Getnode(c.root)
	if err != nil {
		return err
	}

	insertionIndex, nodeToInsertIn, ancestorsIndexes, err := root.Findkey(i.Key, false)
	if err != nil {
		return err
	}

	if nodeToInsertIn.Items != nil && insertionIndex < len(nodeToInsertIn.Items) && bytes.Equal(nodeToInsertIn.Items[insertionIndex].Key, key) {
		return errors.New("[Error]:this key already excists in the key-value store")
	} else {
		nodeToInsertIn.addItem(i, insertionIndex)
	}
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
		if node.isOverPopulated() {
			fmt.Println("Calling split on: ", len(node.Items))
			pnode.split(node, nodeIndex)
		}
	}

	rootNode := ancestors[0]
	if rootNode.isOverPopulated() {
		newNode := c.DAL.nodeCreate([]*Item{}, []pgNum{rootNode.Pagenum})
		fmt.Println("Calling split on: ", len(rootNode.Items))
		newNode.split(rootNode, 0)
		newRoot, err := c.DAL.Writenode(newNode)
		if err != nil {
			return err
		}
		c.root = newRoot.Pagenum
		c.DAL.Meta.Root = newRoot.Pagenum
		c.DAL.Writemeta(c.DAL.Meta)
	}
	return nil
}

func (c *Collection) GetNodes(indexes []int) ([]*Node, error) {
	root, err := c.DAL.Getnode(c.root)
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
	rootNode, err := c.DAL.Getnode(c.root)
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
		c.root = ancestors[1].Pagenum
	}

	return nil
}
