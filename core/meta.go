package core

import "encoding/binary"

const (
	metaPageNum = 0
)

// Meta is the Meta page of the db
type Meta struct {
	freelistPage pgNum
}

func newMetaPage() *Meta {
	return &Meta{}
}

func (m *Meta) Serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

func (m *Meta) Deserialize(buf []byte) {
	pos := 0
	m.freelistPage = pgNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}