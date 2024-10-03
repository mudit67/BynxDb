package core

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
