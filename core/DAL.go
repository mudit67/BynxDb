package core

import (
	"fmt"
	"os"
)

type pgNum uint64

type page struct {
	Num  pgNum
	Data []byte
}

type DAL struct {
	file     *os.File
	pageSize int
	*freeList
}

func DalCreate(path string, pageSize int) (*DAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dal := &DAL{file, pageSize, freeListCreate()}
	return dal, nil
}

func (d *DAL) Close() error {
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil
	}
	return nil
}

func (d *DAL) AllocateEmptyPage() *page {
	return &page{
		Data: make([]byte, d.pageSize),
	}
}

func (d *DAL) ReadPage(pageNum pgNum) (*page, error) {
	p := d.AllocateEmptyPage()

	offset := int(pageNum) * d.pageSize

	if _, err := d.file.WriteAt(p.Data, int64(offset)); err != nil {
		return nil, err
	}
	return p, nil
}

func (d *DAL) WritePage(p *page) error {
	offset := int64(p.Num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.Data, offset)
	return err
}
