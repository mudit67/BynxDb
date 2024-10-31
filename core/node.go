package core

import (
	"bytes"
	"encoding/binary"
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

	// If we reached a leaf node
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

// elementSize returns the size of a key-value-childNode triplet at a given index.
// If the node is a leaf, then the size of a key-value pair is returned.
// It's assumed i <= len(n.items)
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.Items[i].Key)
	size += len(n.Items[i].Value)
	size += pageNumSize
	return size
}

// nodeSize returns the node's size in bytes
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

func (parentNode *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	splitIndex := parentNode.DAL.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.Items[splitIndex]
	var newNode *Node
	if nodeToSplit.Isleaf() {
		newNode, _ = parentNode.Writenode(parentNode.DAL.nodeCreate(nodeToSplit.Items[splitIndex+1:], []pgNum{}))
		nodeToSplit.Items = nodeToSplit.Items[:splitIndex]
	} else {
		newNode, _ = parentNode.Writenode(parentNode.DAL.nodeCreate(nodeToSplit.Items[splitIndex+1:], newNode.Childnodes[splitIndex+1:]))
		nodeToSplit.Items = nodeToSplit.Items[:splitIndex]
		nodeToSplit.Childnodes = nodeToSplit.Childnodes[:splitIndex+1]
	}

	parentNode.addItem(middleItem, nodeToSplitIndex)

	if len(parentNode.Childnodes) == nodeToSplitIndex+1 {
		parentNode.Childnodes = append(parentNode.Childnodes, newNode.Pagenum)
	} else {
		parentNode.Childnodes = append(parentNode.Childnodes[:nodeToSplitIndex+1], parentNode.Childnodes[nodeToSplitIndex:]...)
		parentNode.Childnodes[nodeToSplitIndex] = newNode.Pagenum
	}

	parentNode.Writenodes(parentNode, nodeToSplit)

}
