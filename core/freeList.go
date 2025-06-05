package core

import (
	"BynxDB/core/utils"
	"encoding/binary"
	"fmt"
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
	utils.Info(2, "Max Page: ", fL.maxPage)
	return (fL.maxPage - 1)
}

func (fL *freeList) ReleasedPage(pageNum pgNum) {
	fL.releasedPages = append(fL.releasedPages, pageNum)
}

func (fL *freeList) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16((fL.maxPage)))

	pos += 2

	// * Released page count
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

func (fl *freeList) State() (ret string) {
	ret += "Max Page: " + fmt.Sprint(fl.maxPage)
	ret += " Released Pages:"
	for _, v := range fl.releasedPages {
		ret += " " + fmt.Sprint(v)
	}
	return
}
