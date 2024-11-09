package core

import (
	"BynxDB/core/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

type DB struct {
	records           *Collection
	uniqueColumnsTree []*Collection
	// index   []*Collection
}

func DbInit(name string, tD *TableDef) (*DB, error) {
	name = strings.ToLower(name)
	db := &DB{}
	var err error
	db.records, err = CollectionCreate([]byte(name+"rec"), tD)
	if err != nil {
		return nil, err
	}
	for _, colIndex := range db.records.TableDef.UniqueCols {
		fmt.Println("Creating Index Tree for column: ", colIndex)
		indexTableDef := &TableDef{
			Types: []uint16{db.records.TableDef.Types[colIndex], db.records.TableDef.Types[0]},
			Cols:  []string{db.records.TableDef.Cols[colIndex], db.records.TableDef.Cols[0]},
		}
		tmpCol, err := CollectionCreate([]byte(name+db.records.TableDef.Cols[colIndex]), indexTableDef)
		if err != nil {
			return nil, err
		}
		db.uniqueColumnsTree = append(db.uniqueColumnsTree, tmpCol)
	}
	return db, nil
}

func (db *DB) Insert(valuesToInsert ...any) error {
	// fmt.Println("Inserting: ", valuesToInsert)
	if len(valuesToInsert) != len(db.records.TableDef.Cols) {
		return errors.New("[Error]:too few or too many columns")
	}
	pKey := make([]byte, 0)
	value := make([]byte, 0)
	pKey, err := checkTypeAndEncodeByte(db.records.TableDef, 0, valuesToInsert[0], pKey)
	if err != nil {
		return err
	}

	for i := 1; i < len(valuesToInsert); i++ {
		value, err = checkTypeAndEncodeByte(db.records.TableDef, i, valuesToInsert[i], value)
		if err != nil {
			return err
		}
	}
	// fmt.Println(string(value))
	for i, col := range db.records.UniqueCols {
		indexCollection := db.uniqueColumnsTree[i]
		indexKey, err := checkTypeAndEncodeByte(db.records.TableDef, col, valuesToInsert[col], []byte{})
		if err != nil {
			return err
		}
		err = indexCollection.Put(indexKey, pKey)
		if err != nil {
			return err
		}
	}
	err = db.records.Put(pKey, value)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) PKeyQuery(val any) ([]any, error) {
	key, err := checkTypeAndEncodeByte(db.records.TableDef, 0, val, []byte{})
	if err != nil {
		return nil, err
	}
	fmt.Println(val, key)
	it, err := db.records.Find(key)
	if err != nil {
		return nil, err
	}
	if it == nil {
		return nil, errors.New("[error] row not found")
	}
	// fmt.Println(it)
	return decodeRow(db.records.TableDef, it.Value), nil
}

func (db *DB) PointQuery(colIndex int, val any) ([][]any, error) {
	for _, col := range db.records.UniqueCols {
		if colIndex == col {
			row, err := db.PointQueryUniqueCol(colIndex, val)
			if err != nil {
				return nil, err
			}
			return [][]any{row}, err
		}
	}
	_, err := checkTypeAndEncodeByte(db.records.TableDef, colIndex, val, []byte{})
	if err != nil {
		return nil, err
	}
	items, err := db.records.FetchAll(0)
	if err != nil {
		return nil, err
	}
	var rows [][]any
	for _, item := range items {
		row := decodeRow(db.records.TableDef, item.Value)
		if db.records.TableDef.Types[colIndex] == TYPE_BYTE && bytes.Equal(row[colIndex-1].([]byte), val.([]byte)) {
			rows = append(rows, row)
		} else if row[colIndex-1] == val {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (db *DB) PointQueryUniqueCol(colIndex int, val any) ([]any, error) {
	var collectionIndex int = -1
	for i, col := range db.records.TableDef.UniqueCols {
		if col == colIndex {
			collectionIndex = i
		}
	}
	if collectionIndex == -1 {
		return nil, errors.New("[error] not a unique column")
	}
	key, err := checkTypeAndEncodeByte(db.records.TableDef, colIndex, val, []byte{})
	if err != nil {
		return nil, err
	}
	it, err := db.uniqueColumnsTree[collectionIndex].Find(key)
	if err != nil {
		return nil, err
	}
	if it == nil {
		return nil, errors.New("[error] value not found")
	}
	fmt.Println(db.records.TableDef.Types[0])
	if db.records.TableDef.Types[0] == TYPE_INT64 {
		return db.PKeyQuery(int(binary.LittleEndian.Uint64(it.Value)))
		// fmt.Println()
	}
	return db.PKeyQuery(it.Value)
}

func (db *DB) Close() {
	// fmt.Println(db.records.TableDef)
	db.records.Close()
}

// func filterRow(tD *TableDef, cols []any, colIndex int, val any) bool {

// }

func decodeRow(tD *TableDef, buf []byte) []any {
	var row []any
	leftPos := 0
	for i := 1; i < len(tD.Cols); i++ {
		col, offset := checkTypeAndDecodeCol(tD, i, buf[leftPos:])
		row = append(row, col)
		leftPos += offset
	}
	return row
}

func checkTypeAndDecodeCol(tD *TableDef, colIndex int, buf []byte) (any, int) {
	switch tD.Types[colIndex] {
	case TYPE_INT64:
		{
			i64 := utils.GetInt(buf)
			return i64, 8
		}
	case TYPE_BYTE:
		{
			bufToReturn, offset := utils.GetByte(buf)
			return bufToReturn, offset
		}
	default:
		{
			return nil, 0
		}
	}
}
func checkTypeAndEncodeByte(tD *TableDef, colIndex int, val any, buf []byte) ([]byte, error) {
	switch data := val.(type) {
	case int:
		if tD.Types[colIndex] != TYPE_INT64 {
			return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
		}
		buf = utils.AddInt(buf, data)
	case []byte:
		if tD.Types[colIndex] != TYPE_BYTE {
			return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
		}
		buf = utils.AddByte(buf, data)
	default:
		fmt.Println("Data Type: ", val)
		return nil, errors.New("[Error]:wrong data type passed to function")
	}
	return buf, nil
}
