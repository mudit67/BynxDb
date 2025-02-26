package utils

import (
	"encoding/binary"
	// "fmt"
)

func AddInt(buf []byte, val int) []byte {
	/*
	* byte size:     |  8  |
	* int coloumn:   | val |
	 */

	buf = binary.LittleEndian.AppendUint64(buf, uint64(val))
	return buf
}

func AddByte(buf []byte, val []byte) []byte {
	/*
	*	byte size:     |  2  -  x |
	*	[]byte coloumn |size - val|

	 */
	x := len(val)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(x))
	buf = append(buf, val...)
	return buf
}
