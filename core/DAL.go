package core

import (
	"BynxDB/core/utils"
	"errors"
	"fmt"
	"os"
)

type pgNum uint64

const (
	pageNumSize    = 8
	nodeHeaderSize = 3
)

type page struct {
	Num  pgNum
	Data []byte
}

type Options struct {
	PageSize       int
	MinFillPercent float32
	MaxFillPercent float32
}

var DefaultOptions = &Options{
	MinFillPercent: 0.5,
	MaxFillPercent: 0.95,
}

type DAL struct {
	file           *os.File
	pageSize       int
	MinFillPercent float32
	MaxFillPercent float32

	*freeList
	*Meta
}

func DalCreate(path string, options *Options) (*DAL, error) {
	dal := &DAL{Meta: newMetaPage(), pageSize: options.PageSize, MinFillPercent: options.MinFillPercent, MaxFillPercent: options.MaxFillPercent}
	// * If a database exists
	if _, err := os.Stat(path); err == nil {
		// // fmt.println("Database Exists")
		utils.Info(1, "Database Exists")
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}
		Meta, err := dal.Readmeta()

		if err != nil {
			_ = dal.Close()
			return nil, err
		}

		dal.Meta = Meta

		freeList, err := dal.Readfreelist()

		if err != nil {
			return nil, err
		}
		// // fmt.println(dal.Root)
		dal.freeList = freeList
		utils.Info(1, "Loaded Database: ", "Freelist: ", dal.freelistPage, "TableDef: ", dal.TableDefPage, "Root: ", dal.Root)
	} else if errors.Is(err, os.ErrNotExist) { // *Creating Database
		utils.Info(1, "Creating new Database")
		_, err := os.Stat("./db/")
		if os.IsNotExist(err) {
			err = os.Mkdir("./db/", 0777)
			if err != nil {
				_ = dal.Close()
				return nil, err
			}
		}
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}
		dal.freeList = freeListCreate()
		dal.freelistPage = dal.GetNextPage()
		if _, err := dal.Writefreelist(); err != nil {
			return nil, err
		}
		if _, err := dal.Writemeta(dal.Meta); err != nil {
			return nil, err
		}
		utils.Info(1, "New Database: ", "Freelist: ", dal.freelistPage, "TableDef: ", dal.TableDefPage, "Root: ", dal.Root)
	} else {
		return nil, err
	}

	return dal, nil
}

func (d *DAL) Close() error {
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return fmt.Errorf("Could not close file: %s", err)
		}
		d.file = nil
	}
	return nil
}

// * Page Auxi Functions

// * Allocate space in memort the size of a page in disk
func (d *DAL) Allocateemptypage() *page {
	return &page{
		Data: make([]byte, d.pageSize),
	}
}

func (d *DAL) Readpage(pageNum pgNum) (*page, error) {
	p := d.Allocateemptypage()

	offset := int(pageNum) * d.pageSize

	if _, err := d.file.ReadAt(p.Data, int64(offset)); err != nil {
		return nil, err
	}
	return p, nil
}

func (d *DAL) Writepage(p *page) error {
	utils.Info(4, "Writing Page: ", p.Num)
	offset := int64(p.Num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.Data, offset)
	return err
}

// * (Maintaining) Persistance Auxi Functions

func (d *DAL) Writemeta(metaToWrite *Meta) (*page, error) {
	utils.Info(1, "Writing Meta: ", "Freelist: ", metaToWrite.freelistPage, "TableDef: ", metaToWrite.TableDefPage, "Root: ", metaToWrite.Root)
	p := d.Allocateemptypage()
	p.Num = metaPageNum

	metaToWrite.Serialize(p.Data)

	if err := d.Writepage(p); err != nil {
		return nil, err
	}
	return p, nil
}

func (d *DAL) Readmeta() (*Meta, error) {
	utils.Info(1, "Reading meta page: ", metaPageNum)
	p, err := d.Readpage(metaPageNum)

	if err != nil {
		return nil, err
	}

	Meta := newMetaPage()
	Meta.Deserialize(p.Data)
	return Meta, nil
}

func (d *DAL) Writefreelist() (*page, error) {
	p := d.Allocateemptypage()
	p.Num = d.freelistPage
	d.freeList.serialize(p.Data)
	utils.Info(1, "Writing Freelist: ", d.freeList.State())
	if err := d.Writepage(p); err != nil {
		return nil, err
	}
	d.freelistPage = p.Num
	return p, nil

}

func (d *DAL) Readfreelist() (*freeList, error) {
	utils.Info(1, "Reading Freelist.")
	p, err := d.Readpage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freeList := freeListCreate()

	freeList.deserialize(p.Data)
	utils.InfoLogAndPrint("Reading Freelist: ", freeList.State())
	return freeList, nil

}

// * B-tree Struct Auxi Functions

func (d *DAL) nodeCreate(items []*Item, childNodes []pgNum) *Node {
	node := NodeCreate()
	node.Items = items
	node.Childnodes = childNodes
	node.DAL = d
	node.Pagenum = d.GetNextPage()
	return node
}

func (d *DAL) Getnode(pageNum pgNum) (*Node, error) {
	p, err := d.Readpage(pageNum)
	if err != nil {
		return nil, err
	}
	node := NodeCreate()
	node.Deserialize(p.Data)
	node.Pagenum = pageNum
	node.DAL = d
	return node, nil
}

func (d *DAL) Writenode(n *Node) (*Node, error) {
	p := d.Allocateemptypage()
	if n.Pagenum == 0 {
		utils.Warn("Writing Node With 0 Pg num")
		p.Num = d.GetNextPage()
		n.Pagenum = p.Num
	} else {
		p.Num = n.Pagenum
	}
	p.Data = n.Serialize(p.Data)
	err := d.Writepage(p)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (d *DAL) Deletenode(pageNum pgNum) {
	d.ReleasedPage(pageNum)
}

// * Btree rules maintainance Auxi Functions.

func (d *DAL) maxThreshold() float32 {
	return d.MaxFillPercent * float32(d.pageSize)
}

func (d *DAL) isOverPopulated(node *Node) bool {
	return float32(node.nodeSize()) > d.maxThreshold()
}

func (d *DAL) minThreshold() float32 {
	return d.MinFillPercent * float32(d.pageSize)
}

func (d *DAL) isUnderPopulated(node *Node) bool {
	return float32(node.nodeSize()) < d.minThreshold()
}

// * Return the index + 1 of the Item till which the minThreshold of a nodeSize hold true.
func (d *DAL) getSplitIndex(node *Node, splitNec bool) int {
	size := nodeHeaderSize
	minSize := d.minThreshold()
	// fmt.println(minSize)
	for i := range node.Items {
		size += node.elementSize(i)
		// fmt.println(size, i)
		if float32(size) > minSize && i < len(node.Items)-1 {
			return i + 1
		}
	}
	if splitNec && len(node.Items) == 2 {
		return 1
	}
	return -1
}
