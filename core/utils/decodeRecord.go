package utils

import "encoding/binary"

func GetInt(buf []byte) int {
	/*
		byte size:     |  8  |
		int coloumn:   | val |
	*/

	return int(binary.LittleEndian.Uint64(buf))
}

func GetByte(buf []byte) ([]byte, int) {
	/*
		byte size:     |  2  -  x |
		[]byte coloumn |size - val|

	*/
	leftPos := 0
	byteSize := int(binary.LittleEndian.Uint16(buf))
	leftPos += 2
	retBuf := make([]byte, byteSize)
	copy(retBuf, buf[leftPos:leftPos+byteSize])
	leftPos += byteSize
	return retBuf, leftPos
}
