package core

import (
	"BynxDB/core/utils"
	"errors"
	"fmt"
)

type DB struct {
	records *Collection
	// index   []*Collection
}

func DbInit(name string, tD *TableDef) (*DB, error) {
	db := &DB{}
	var err error
	db.records, err = CollectionCreate([]byte(name+"Rec"), tD)
	if err != nil {
		return nil, err
	}
	// pKeyType, pKeyName := tD.Types[tD.pKey], tD.Cols[tD.pKey]
	// db.pKeyIndex, err := CollectionCreate([]byte(name + pKeyName))
	return db, nil
}

func (db *DB) Insert(check ...any) error {
	if len(check) != len(db.records.TableDef.Cols) {
		return errors.New("[Error]:too few or too many columns")
	}
	key := make([]byte, 0)
	value := make([]byte, 0)
	key, err := checkTypeAndEncodeByte(db.records.TableDef, 0, check[0], key)
	if err != nil {
		return err
	}

	for i := 1; i < len(check); i++ {
		value, err = checkTypeAndEncodeByte(db.records.TableDef, i, check[i], value)
		if err != nil {
			return err
		}
	}
	fmt.Println(string(value))
	err = db.records.Put(key, value)
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
	it, err := db.records.Find(key)
	if err != nil {
		return nil, err
	}
	fmt.Println(it)
	var row []any
	leftPos := 0
	for i := 1; i < len(db.records.TableDef.Cols); i++ {
		col, offset := checkTypeAndDecodeCol(db.records.TableDef, i, it.Value[leftPos:])
		row = append(row, col)
		leftPos += offset
	}
	return row, nil
}

func (db *DB) Close() {
	fmt.Println(db.records.TableDef)
	db.records.Close()
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
	switch val.(type) {
	case int:
		if tD.Types[colIndex] != TYPE_INT64 {
			return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
		}
		buf = utils.AddInt(buf, val.(int))
	case []byte:
		if tD.Types[colIndex] != TYPE_BYTE {
			return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
		}
		buf = utils.AddByte(buf, val.([]byte))
	default:
		fmt.Println(val)
		return nil, errors.New("[Error]:wrong data type passed to function")
	}
	return buf, nil
}
