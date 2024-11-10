package core

import "encoding/binary"

const (
	metaPageNum = 0
)

// * Meta is the Meta page of the db
type Meta struct {
	Root         pgNum
	freelistPage pgNum
	TableDefPage pgNum
}

func newMetaPage() *Meta {
	return &Meta{}
}

func (m *Meta) Serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.Root))
	pos += pageNumSize
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.TableDefPage))
	pos += pageNumSize
}

func (m *Meta) Deserialize(buf []byte) {
	pos := 0

	m.Root = pgNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize

	m.freelistPage = pgNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize

	m.TableDefPage = pgNum(binary.LittleEndian.Uint64(buf[pos:]))
}
