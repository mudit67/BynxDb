package core

import (
	"encoding/binary"
)

type freeList struct {
	maxPage       pgNum
	releasedPages []pgNum
}

const initialPage = 1

func freeListCreate() *freeList {
	return &freeList{
		maxPage:       initialPage,
		releasedPages: []pgNum{},
	}
}

func (fL *freeList) GetNextPage() pgNum {
	if len(fL.releasedPages) != 0 {
		freePgNum := fL.releasedPages[len(fL.releasedPages)-1]
		fL.releasedPages = fL.releasedPages[:len(fL.releasedPages)-1]
		return freePgNum
	}
	fL.maxPage += 1
	return fL.maxPage
}

func (fL *freeList) ReleasedPage(pageNum pgNum) {
	fL.releasedPages = append(fL.releasedPages, pageNum)
}

func (fL *freeList) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16((fL.maxPage)))

	pos += 2

	// Released page count
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(fL.releasedPages)))
	pos += 2

	for _, page := range fL.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}

	return buf

}

func (fL *freeList) deserialize(buf []byte) {
	pos := 0
	fL.maxPage = pgNum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	releasedPageCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPageCount; i++ {
		fL.releasedPages = append(fL.releasedPages, pgNum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}

}
