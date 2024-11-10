package core

import "encoding/binary"

const (
	TYPE_INT64 = 1
	TYPE_BYTE  = 2
)

/*
* Stores the structure and definition of a table. The primary key will always be stored in index 0. If the pKeyIndex != 0, the columns will be swapped
 */
type TableDef struct {
	Types     []uint16
	Cols      []string
	PKeyIndex int //Starting with 0/
	/*
	*  Indices of columns that have the contraint of being unique.	This tells the database to create a index Tree for that specific column. Starts with 0
	 */
	UniqueCols []int
}

func (tD *TableDef) Serialize(buf []byte) []byte {
	/*
	*	| Total Number of Columns | Columns' Types | Columns' Names | Number of Unique Columns | Indices of Unique Columns |
	 */
	leftPos := 0
	numOfCol := len(tD.Cols)
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(numOfCol))
	leftPos += 2
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

	noUniqueColumns := len(tD.UniqueCols)
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(noUniqueColumns))
	leftPos += 2

	for _, col := range tD.UniqueCols {
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(col))
		leftPos += 2
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
	noUniqueColumns := int(binary.LittleEndian.Uint16(buf[leftPos:]))
	leftPos += 2

	for i := 0; i < noUniqueColumns; i++ {
		tD.UniqueCols = append(tD.UniqueCols, int(binary.LittleEndian.Uint16(buf[leftPos:])))
		leftPos += 2
	}

}
