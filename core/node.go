package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Item struct {
	Key   []byte
	Value []byte
}

type Node struct {
	*DAL

	Pagenum    pgNum
	Items      []*Item
	Childnodes []pgNum
}

// Init Functions

func NodeCreate() *Node {
	return &Node{}
}

func ItemCreate(Key []byte, Value []byte) *Item {
	return &Item{
		Key:   Key,
		Value: Value,
	}
}

func (n *Node) Isleaf() bool {
	return len(n.Childnodes) == 0
}

// * DB auxi Functions

func (n *Node) Serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	Isleaf := n.Isleaf()
	var bitSetVar uint64
	if Isleaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.Items)))
	leftPos += 2

	for i := 0; i < len(n.Items); i++ {
		item := n.Items[i]
		if !Isleaf {
			childNode := n.Childnodes[i]

			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize

		}
		kLen := len(item.Key)
		vLen := len(item.Value)

		offset := rightPos - (kLen + vLen + 2)
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vLen
		copy(buf[rightPos:], item.Value)

		rightPos -= 1
		buf[rightPos] = byte(vLen)

		rightPos -= kLen
		copy(buf[rightPos:], item.Key)

		rightPos -= 1
		buf[rightPos] = byte(kLen)
	}
	if !Isleaf {
		lastChildNode := n.Childnodes[len(n.Childnodes)-1]
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}
	return buf
}

func (n *Node) Deserialize(buf []byte) {
	leftPos := 0

	Isleaf := uint16(buf[0])
	ItemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))

	leftPos += 3

	for i := 0; i < ItemsCount; i++ {
		if Isleaf == 0 {
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize
			n.Childnodes = append(n.Childnodes, pgNum(pageNum))
		}

		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		kLen := uint16(buf[int(offset)])
		offset += 1

		Key := buf[offset : offset+kLen]
		offset += kLen

		vLen := uint16(buf[int(offset)])
		offset += 1

		Value := buf[offset : offset+vLen]
		offset += vLen

		n.Items = append(n.Items, ItemCreate(Key, Value))
	}

	if Isleaf == 0 {
		pageNum := pgNum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.Childnodes = append(n.Childnodes, pageNum)
	}
}

func (n *Node) Writenode(node *Node) (*Node, error) {
	node, _ = n.DAL.Writenode(node)
	return node, nil
}

func (n *Node) Writenodes(nodes ...*Node) {
	for _, node := range nodes {
		n.Writenode(node)
	}
}

func (n *Node) Getnode(pageNum pgNum) (*Node, error) {
	return n.DAL.Getnode(pageNum)
}

// * Searching Functions

func (n *Node) Findkeyinnode(Key []byte) (bool, int) {
	for i, existingItem := range n.Items {
		res := bytes.Compare(existingItem.Key, Key)
		if res == 0 {
			return true, i
		}
		if res == 1 {
			return false, i
		}

	}
	return false, len(n.Items)
}
func (n *Node) Findkey(Key []byte, exact bool) (int, *Node, []int, error) {
	ancestorIndexes := &[]int{0}
	index, node, err := Findkeyhelper(n, Key, exact, ancestorIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, *ancestorIndexes, nil
}

func Findkeyhelper(n *Node, Key []byte, exact bool, ancestorIndexes *[]int) (int, *Node, error) {
	wasFound, index := n.Findkeyinnode(Key)
	if wasFound {
		return index, n, nil
	}

	// * If we reached a leaf node
	if n.Isleaf() {
		if exact {
			return -1, nil, nil
		}
		return index, n, nil
	}

	*ancestorIndexes = append(*ancestorIndexes, index)
	nextChild, err := n.Getnode(n.Childnodes[index])
	if err != nil {
		return -1, nil, err
	}
	return Findkeyhelper(nextChild, Key, exact, ancestorIndexes)
}

// * Insertion Auxi Functions

// * elementSize returns the size of a key-value-childNode triplet at a given index.
// * If the node is a leaf, then the size of a key-value pair is returned.
// * It's assumed i <= len(n.items)
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.Items[i].Key)
	size += len(n.Items[i].Value)
	size += pageNumSize
	return size
}

// * nodeSize returns the node's size in bytes
func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.Items {
		size += n.elementSize(i)
	}

	size += pageNumSize

	return size
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.Items) == insertionIndex {
		n.Items = append(n.Items, item)
	} else {
		n.Items = append(n.Items[:insertionIndex+1], n.Items[insertionIndex:]...)
		n.Items[insertionIndex] = item
	}
	return insertionIndex
}

func (n *Node) isUnderPopulated() bool {
	return n.DAL.isUnderPopulated(n)
}

func (n *Node) isOverPopulated() bool {
	return n.DAL.isOverPopulated(n)
}

// * note: split() is responsible for creating new levels & by extenstion new nodes in the B-tree

func (parentNode *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	//* split rebalances the tree after adding. After insertion the modified node has to be checked to make sure it
	//* didn't exceed the maximum number of elements. If it did, then it has to be split and rebalanced. The transformation
	//* is depicted in the graph below. If it's not a leaf node, then the children has to be moved as well as shown.
	//* This may leave the parent unbalanced by having too many items so rebalancing has to be checked for all the ancestors.
	//* The split is performed in a for loop to support splitting a node more than once. (Though in practice used only once).
	//*
	//*		        parentNode                              parentNode
	//*	                3                                       3,6
	//*		      /        \           ------>       /          |          \
	//*		   a           modifiedNode            a       modifiedNode     newNode
	//*	  1,2                 4,5,6,7,8            1,2          4,5         7,8
	splitIndex := parentNode.DAL.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.Items[splitIndex]
	var newNode *Node
	if nodeToSplit.Isleaf() {
		newNode, _ = parentNode.Writenode(parentNode.DAL.nodeCreate(nodeToSplit.Items[splitIndex+1:], []pgNum{}))
		nodeToSplit.Items = nodeToSplit.Items[:splitIndex]
	} else {
		newNode, _ = parentNode.Writenode(parentNode.DAL.nodeCreate(nodeToSplit.Items[splitIndex+1:], nodeToSplit.Childnodes[splitIndex+1:]))
		nodeToSplit.Items = nodeToSplit.Items[:splitIndex]
		nodeToSplit.Childnodes = nodeToSplit.Childnodes[:splitIndex+1]
	}

	parentNode.addItem(middleItem, nodeToSplitIndex)
	fmt.Println("Writing child nodes: ", len(parentNode.Childnodes), nodeToSplitIndex)
	if len(parentNode.Childnodes) == nodeToSplitIndex+1 {

		parentNode.Childnodes = append(parentNode.Childnodes, newNode.Pagenum)
	} else {
		parentNode.Childnodes = append(parentNode.Childnodes[:nodeToSplitIndex+1], parentNode.Childnodes[nodeToSplitIndex:]...)
		fmt.Println(parentNode.Childnodes)
		parentNode.Childnodes[nodeToSplitIndex] = newNode.Pagenum
	}

	parentNode.Writenodes(parentNode, nodeToSplit)

}

// * Deletion Auxi Functions

func (n *Node) canSpareAnElement() bool {
	// TODO: Don't use getSplitIndex here
	splitIndex := n.getSplitIndex(n)
	return splitIndex != -1
}

func (n *Node) removeItemFromLeaf(index int) {
	noOfItems := len(n.Items)
	if index >= noOfItems {
		return
	} else if index == noOfItems-1 {
		n.Items = n.Items[:index]
		n.Writenode(n)
	} else {

		n.Items = append(n.Items[:index], n.Items[index+1:]...)
		n.Writenode(n)
	}
}

func (n *Node) removeItemFromInternal(index int) ([]int, error) {
	//*          p
	//*       /
	//*     ..
	//*  /     \
	//* ..      a

	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)

	aNode, err := n.Getnode(n.Childnodes[index])
	if err != nil {
		return nil, err
	}
	for !aNode.Isleaf() {
		// EXP
		traversingIndex := len(aNode.Childnodes) - 1
		aNode, err = aNode.Getnode(aNode.Childnodes[traversingIndex])
		if err != nil {
			return nil, err
		}
		affectedNodes = append(affectedNodes, traversingIndex)
	}
	n.Items[index] = aNode.Items[len(aNode.Items)-1]
	aNode.removeItemFromLeaf(len(aNode.Items) - 1)

	return affectedNodes, nil
}

func rightRotate(aNode, bNode, pNode *Node, bNodeIndex int) {
	//* 	           p                                    p
	//*                4                                    3
	//*	          /         \           ------>         /         \
	//*	         a           b (unbalanced)            a           b (unbalanced)
	//*      1,2,3            5                      1,2            4,5

	aNodeLItem := aNode.Items[len(aNode.Items)-1]
	aNode.Items = aNode.Items[:len(aNode.Items)-1]

	pNodeItemIndex := bNodeIndex - 1
	if bNodeIndex == 0 {
		pNodeItemIndex = 0
	}

	pNodeItem := pNode.Items[pNodeItemIndex]
	pNode.Items[pNodeItemIndex] = aNodeLItem

	bNode.Items = append([]*Item{pNodeItem}, bNode.Items...)

	if !aNode.Isleaf() {
		childNodeToShift := aNode.Childnodes[len(aNode.Childnodes)-1]
		aNode.Childnodes = aNode.Childnodes[:len(aNode.Childnodes)-1]
		bNode.Childnodes = append([]pgNum{childNodeToShift}, bNode.Childnodes...)
	}

}

func leftRotate(aNode, bNode, pNode *Node, bNodeIndex int) {
	//*            p                                     p
	//*            2                                     3
	//*	      /        \           ------>         /          \
	//*  a(unbalanced)  b                 a(unbalanced)        b
	//* 1              3,4,5                   1,2             4,5

	bNodeItem := bNode.Items[0]
	bNode.Items = bNode.Items[1:]

	pNodeItemIndex := bNodeIndex
	if pNodeItemIndex == len(pNode.Items) {
		pNodeItemIndex = pNodeItemIndex - 1
	}

	pNodeItem := pNode.Items[pNodeItemIndex]
	pNode.Items[pNodeItemIndex] = bNodeItem

	aNode.Items = append(aNode.Items, pNodeItem)

	if !bNode.Isleaf() {
		childNodeToShift := bNode.Childnodes[0]
		bNode.Childnodes = bNode.Childnodes[1:]
		aNode.Childnodes = append(aNode.Childnodes, childNodeToShift)
	}

}

func (n *Node) merge(bNode *Node, bNodeIndex int) error {
	//*                p                                     p
	//*               3,5                                    5
	//*	      /        |       \       ------>         /          \
	//*      a          b        c                    a            c
	//*     1,2         4        6,7               1,2,3,4         6,7
	fmt.Println("Merging nodes")
	aNode, err := n.Getnode(n.Childnodes[bNodeIndex-1])
	if err != nil {
		return err
	}

	pNodeItem := n.Items[bNodeIndex-1]
	n.Items = append(n.Items[:bNodeIndex-1], n.Items[bNodeIndex:]...)
	aNode.Items = append(aNode.Items, pNodeItem)
	n.Childnodes = append(n.Childnodes[:bNodeIndex], n.Childnodes[bNodeIndex+1:]...)

	if !aNode.Isleaf() {
		aNode.Childnodes = append(aNode.Childnodes, bNode.Childnodes...)
	}

	n.Writenodes(aNode, n)
	n.DAL.Deletenode(bNode.Pagenum)

	return nil
}

// * rebalanceRemove rebalances the tree after a remove operation. This can be either by rotating to the right, to the
// * left or by merging. First, the sibling nodes are checked to see if they have enough items for rebalancing
// * (>= minItems+1). If they don't have enough items, then merging with one of the sibling nodes occurs. This may leave
// * the parent unbalanced by having too little items so rebalancing has to be checked for all the ancestors.
func (n *Node) rebalanceRemove(unbalancedNode *Node, unbalancedNodeIndex int) error {
	pNode := n
	var rightNode *Node
	// *Check if right Rotate is feasible
	if unbalancedNodeIndex != 0 {
		leftNode, err := pNode.Getnode(pNode.Childnodes[unbalancedNodeIndex-1])
		if err != nil {
			return err
		}
		if leftNode.canSpareAnElement() {
			rightRotate(leftNode, unbalancedNode, pNode, unbalancedNodeIndex)
			pNode.Writenodes(leftNode, unbalancedNode, pNode)
			return nil
		}
	}

	// *left Rotate
	if unbalancedNodeIndex != len(pNode.Childnodes)-1 {
		var err error
		rightNode, err = pNode.Getnode(pNode.Childnodes[unbalancedNodeIndex+1])
		if err != nil {
			return err
		}
		if rightNode.canSpareAnElement() {
			leftRotate(unbalancedNode, rightNode, pNode, unbalancedNodeIndex)
			pNode.Writenodes(rightNode, pNode, unbalancedNode)
		}
	}
	//* The merge function merges a given node with its node to the right. So by default, we merge an unbalanced node
	//* with its left sibling. In the case where the unbalanced node is the leftmost, we have to replace the merge()
	//* parameters, so the unbalanced node right sibling, will be merged into the unbalanced node.
	if unbalancedNodeIndex == 0 {
		if rightNode == nil {
			fmt.Println("right node is nil")
		}
		return pNode.merge(rightNode, unbalancedNodeIndex+1)
	}
	return pNode.merge(unbalancedNode, unbalancedNodeIndex)
}
