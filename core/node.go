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
func (n *Node) Findkey(Key []byte) (int, *Node, error) {
	index, node, err := Findkeyhelper(n, Key)
	if err != nil {
		return -1, nil, err
	}
	return index, node, nil
}

func Findkeyhelper(n *Node, Key []byte) (int, *Node, error) {
	wasFound, index := n.Findkeyinnode(Key)
	if wasFound {
		return index, n, nil
	}
	// If we reached a leaf node
	if n.Isleaf() {
		return -1, nil, nil
	}
	nextChild, err := n.Getnode(n.Childnodes[index])
	if err != nil {
		return -1, nil, err
	}
	return Findkeyhelper(nextChild, Key)
}
