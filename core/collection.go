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
	n, err := c.DAL.Getnode(c.root)
	if err != nil {
		return nil, err
	}
	index, containingNode, _, err := n.Findkey(key, true)
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

	if nodeToInsertIn.Items != nil && insertionIndex < len(nodeToInsertIn.Items) && bytes.Compare(nodeToInsertIn.Items[insertionIndex].Key, key) == 0 {
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
