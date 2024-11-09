package core

import "encoding/binary"

const (
	TYPE_INT64 = 1
	TYPE_BYTE  = 2
)

type TableDef struct {
	Types     []uint16
	Cols      []string
	PKeyIndex int //Starting with 0
	// TableDefPage pgNum
	// RootBtree    pgNum
}

// type Cell struct {
// 	Type uint16
// 	i64  int64
// 	str  []byte
// }

// type Record struct {
// 	Cols []string
// 	vals []Cell
// }

func (tD *TableDef) Serialize(buf []byte) []byte {

	leftPos := 0

	numOfCol := len(tD.Cols)
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(numOfCol))
	leftPos += 2
	// binary.LittleEndian.PutUint64(buf[leftPos:], uint64(tD.RootBtree))
	// leftPos += pageNumSize
	// binary.LittleEndian.PutUint64(buf[leftPos:], uint64(tD.TableDefPage))
	// leftPos += pageNumSize

	for _, typ := range tD.Types {
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(typ))
		leftPos += 2
	}

	for _, colName := range tD.Cols {
		nameSize := len(colName)
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(nameSize))
		leftPos += 2
		copy(buf[leftPos:], []byte(colName))
		leftPos += nameSize

	}
	return buf
}

func (tD *TableDef) Deserialize(buf []byte) {
	leftPos := 0

	numOfCol := int(binary.LittleEndian.Uint16(buf[0:2]))
	leftPos += 2
	tD.Types = tD.Types[:0]
	for i := 0; i < numOfCol; i++ {
		tD.Types = append(tD.Types, binary.LittleEndian.Uint16(buf[leftPos:]))
		leftPos += 2
	}
	tD.Cols = tD.Cols[:0]
	for i := 0; i < numOfCol; i++ {
		nameLen := int(binary.LittleEndian.Uint16(buf[leftPos:]))
		leftPos += 2
		tD.Cols = append(tD.Cols, string(buf[leftPos:leftPos+nameLen]))
		leftPos += nameLen
	}
}
