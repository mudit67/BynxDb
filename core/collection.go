package core

import (
	"bytes"
)

type Collection struct {
	name []byte

	root pgNum
	DAL  *DAL
}

func CollectionCreate(name []byte, root pgNum) *Collection {
	return &Collection{
		name: name,
		root: root,
	}
}

// TODO: Add ancestorsIndexs

func (c *Collection) Find(key []byte) (*Item, error) {
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

func (c *Collection) Put(key []byte, value []byte) error {
	i := ItemCreate(key, value)

	var root *Node
	var err error

	if c.root == 0 {
		nodeTemp := c.DAL.nodeCreate([]*Item{i}, []pgNum{})
		root, err = c.DAL.Writenode(nodeTemp)
		if err != nil {
			return err
		}
		c.root = root.Pagenum
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
		nodeToInsertIn.Items[insertionIndex] = i
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

	// Handle rebalancing
	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]
		if node.isOverPopulated() {
			pnode.split(node, nodeIndex)
		}
	}

	rootNode := ancestors[0]
	if rootNode.isOverPopulated() {
		newNode := c.DAL.nodeCreate([]*Item{}, []pgNum{rootNode.Pagenum})
		newNode.split(rootNode, 0)
		newRoot, err := c.DAL.Writenode(newNode)
		if err != nil {
			return err
		}
		c.root = newRoot.Pagenum
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

// Remove removes a key from the tree. It finds the correct node and the index to remove the item from and removes it.
// When performing the search, the ancestors are returned as well. This way we can iterate over them to check which
// nodes were modified and rebalance by rotating or merging the unbalanced nodes. Rotation is done first. If the
// siblings don't have enough items, then merging occurs. If the root is without items after a split, then the root is
// removed and the tree is one level shorter.
func (c *Collection) Remove(key []byte) error {
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
	// Rebalance the nodes all the way up. Start From one node before the last and go all the way up. Exclude root.
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
	// If the root has no items after rebalancing, there's no need to save it because we ignore it.
	if len(rootNode.Items) == 0 && len(rootNode.Childnodes) > 0 {
		c.root = ancestors[1].Pagenum
	}

	return nil
}
