package core

import (
	"errors"
	"fmt"
	"os"
)

type pgNum uint64

const (
	pageNumSize = 8
)

type page struct {
	Num  pgNum
	Data []byte
}

type DAL struct {
	file     *os.File
	pageSize int
	*freeList
	*Meta
}

func DalCreate(path string, pageSize int) (*DAL, error) {
	dal := &DAL{Meta: newMetaPage()}
	if _, err := os.Stat(path); err == nil {
		// If a database exists
		fmt.Println("Database Exists")
		if dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666); err != nil {
			_ = dal.Close()
			return nil, err
		}
		Meta, err := dal.readMeta()

		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		dal.Meta = Meta

		freeList, err := dal.readFreeList()

		if err != nil {
			return nil, err
		}

		dal.freeList = freeList
	} else if errors.Is(err, os.ErrNotExist) {
		fmt.Println("Creating new Database")
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}
		dal.freeList = freeListCreate()
		dal.freelistPage = dal.GetNextPage()
		// if _, err := dal.WriteFreelist(); err != nil {
		// 	return nil, err
		// }

		// if _, err := dal.writeMeta(dal.Meta); err != nil {
		// 	return nil, err
		// }

	} else {
		return nil, err
	}

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

// Allocate space in memort the size of a page in disk
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

func (d *DAL) writeMeta(Meta *Meta) (*page, error) {
	p := d.AllocateEmptyPage()
	p.Num = metaPageNum

	Meta.Serialize(p.Data)

	if err := d.WritePage(p); err != nil {
		return nil, err
	}
	return p, nil
}

func (d *DAL) readMeta() (*Meta, error) {
	p, err := d.ReadPage(metaPageNum)

	if err != nil {
		return nil, err
	}

	Meta := newMetaPage()
	Meta.Deserialize(p.Data)
	return Meta, nil
}

func (d *DAL) WriteFreelist() (*page, error) {
	p := d.AllocateEmptyPage()
	p.Num = d.freelistPage
	fmt.Println("p.Data: ", p.Data)
	d.freeList.serialize(p.Data)

	if err := d.WritePage(p); err != nil {
		return nil, err
	}
	d.freelistPage = p.Num
	return p, nil

}

func (d *DAL) readFreeList() (*freeList, error) {
	p, err := d.ReadPage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freeList := freeListCreate()

	freeList.deserialize(p.Data)

	return freeList, nil

}
