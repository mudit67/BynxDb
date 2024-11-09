package utils

import (
	"encoding/binary"
	"fmt"
)

func AddInt(buf []byte, val int) []byte {
	/*
		byte size:     |  8  |
		int coloumn:   | val |
	*/

	// var apnd []byte
	binary.LittleEndian.AppendUint64(buf, uint64(val))
	return buf
}

func AddByte(buf []byte, val []byte) []byte {
	/*
		byte size:     |  2  -  x |
		[]byte coloumn |size - val|

	*/
	x := len(val)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(x))
	fmt.Println("Encoded Byte: ", buf)
	// copy(buf[len(buf):], val)
	buf = append(buf, val...)
	fmt.Println("Encoded Byte: ", string(buf))
	return buf
}
