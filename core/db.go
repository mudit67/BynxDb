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
}

func DbInit(name string, tD *TableDef) (*DB, error) {
	name = strings.ToUpper(name)
	for ind, colName := range tD.Cols {
		tD.Cols[ind] = strings.ToUpper(colName)
	}
	db := &DB{}
	var err error
	db.records, err = CollectionCreate([]byte(name+"rec"), tD)
	if err != nil {
		return nil, err
	}
	if len(db.records.TableDef.UniqueCols) != 0 {
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
	}
	return db, nil
}

func (db *DB) Insert(valuesToInsert ...any) error {
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
	for i, col := range db.records.UniqueCols {
		indexCollection := db.uniqueColumnsTree[i]
		indexKey, err := checkTypeAndEncodeByte(db.records.TableDef, col, valuesToInsert[col], []byte{})
		if err != nil {
			return err
		}
		err = indexCollection.Put(indexKey, pKey, false)
		if err != nil {
			return err
		}
	}
	err = db.records.Put(pKey, value, false)
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
	row := decodeRow(db.records.TableDef, it.Value)

	row = append([]any{val}, row...)
	return row, nil
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
			itKey, _ := checkTypeAndDecodeCol(db.records.TableDef, 0, item.Key)
			row = append([]any{itKey}, row...)
			rows = append(rows, row)
		} else if db.records.TableDef.Types[colIndex] == TYPE_INT64 && row[colIndex-1] == val {
			itKey, _ := checkTypeAndDecodeCol(db.records.TableDef, 0, item.Key)
			row = append([]any{itKey}, row...)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (db *DB) PointQueryUniqueCol(colIndex int, val any) ([]any, error) {
	key, collectionIndex, err := checkUniqueColAndEncode(db.records.TableDef, colIndex, val)
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
	// fmt.Println(db.records.TableDef.Types[0])
	if db.records.TableDef.Types[0] == TYPE_INT64 {
		return db.PKeyQuery(int(binary.LittleEndian.Uint64(it.Value)))
	}
	return db.PKeyQuery(it.Value)
}

func (db *DB) SelectEntireTable() ([][]any, error) {
	items, err := db.records.FetchAll(0)
	if err != nil {
		return nil, err
	}
	var rows [][]any
	for _, item := range items {
		row := decodeRow(db.records.TableDef, item.Value)
		itKey, _ := checkTypeAndDecodeCol(db.records.TableDef, 0, item.Key)
		row = append([]any{itKey}, row...)
		rows = append(rows, row)
	}
	return rows, nil
}

func (db *DB) RangeQuery(colIndex int, low any, high any) ([][]any, error) {
	lowKey, err := checkTypeAndEncodeByte(db.records.TableDef, colIndex, low, []byte{})
	if err != nil {
		return nil, err
	}
	highKey, err := checkTypeAndEncodeByte(db.records.TableDef, colIndex, high, []byte{})
	if err != nil {
		return nil, err
	}
	items, err := db.records.FetchAll(0)
	if err != nil {
		return nil, err
	}
	var rows [][]any
	for _, item := range items {
		var keyToCom []byte
		row := decodeRow(db.records.TableDef, item.Value)
		if colIndex == 0 {
			keyToCom = item.Key
		} else {
			if db.records.TableDef.Types[colIndex] == TYPE_INT64 {
				keyToCom = binary.LittleEndian.AppendUint64(keyToCom, uint64(row[colIndex-1].(int)))
			} else {
				keyToCom = row[colIndex-1].([]byte)
			}
		}
		lowCom := bytes.Compare(lowKey, keyToCom)
		highCom := bytes.Compare(highKey, keyToCom)
		// fmt.Println(lowCom, highCom, keyToCom)
		if lowCom <= 0 && highCom >= 0 {
			itKey, _ := checkTypeAndDecodeCol(db.records.TableDef, 0, item.Key)
			row = append([]any{itKey}, row...)
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (db *DB) UpdatePoint(colIndex int, valToChange any, newVal any) error {
	rowsToUpdate, err := db.PointQuery(colIndex, valToChange)
	if err != nil {
		return err
	}
	if rowsToUpdate == nil || len(rowsToUpdate) == 0 {
		return errors.New("no row found to update")
	}
	for ind := range rowsToUpdate {
		rowsToUpdate[ind][colIndex] = newVal

		pKey := make([]byte, 0)
		value := make([]byte, 0)
		pKey, err = checkTypeAndEncodeByte(db.records.TableDef, 0, rowsToUpdate[ind][0], pKey)
		if err != nil {
			return err
		}
		for i := 1; i < len(rowsToUpdate[ind]); i++ {
			value, err = checkTypeAndEncodeByte(db.records.TableDef, i, rowsToUpdate[ind][i], value)
			if err != nil {
				return err
			}
		}
		for i, col := range db.records.UniqueCols {
			indexCollection := db.uniqueColumnsTree[i]
			indexKey, err := checkTypeAndEncodeByte(db.records.TableDef, col, rowsToUpdate[ind][col], []byte{})
			if err != nil {
				return err
			}
			err = indexCollection.Put(indexKey, pKey, true)
			if err != nil {
				return err
			}
		}
		err = db.records.Put(pKey, value, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Delete(colIndex int, val any) error {
	fmt.Println("Deleting: ", val, " In column: ", colIndex)
	key, err := checkTypeAndEncodeByte(db.records.TableDef, colIndex, val, []byte{})
	// * Primary key column
	if colIndex == 0 {
		if err != nil {
			return err
		}
		if len(db.uniqueColumnsTree) != 0 {
			rowToDel, err := db.PKeyQuery(val)
			if err != nil {
				return err
			}
			for i, uniqueColIndex := range db.records.UniqueCols {
				keyToDel, _ := checkTypeAndEncodeByte(db.records.TableDef, uniqueColIndex, rowToDel[uniqueColIndex-1], []byte{})
				err := db.uniqueColumnsTree[i].Remove(keyToDel)
				if err != nil {
					return err
				}
			}
		}
		return db.records.Remove(key)
	} else {
		// * Check unique column
		// uniqueCollectionIndex := -1
		for i, col := range db.records.UniqueCols {
			if col == colIndex {
				item, err := db.uniqueColumnsTree[i].Find(key)
				if err != nil {
					return err
				}
				if item == nil {
					return errors.New("value not found")
				}
				pKey, _ := checkTypeAndDecodeCol(db.records.TableDef, 0, item.Value)
				return db.Delete(0, pKey)
			}
		}
		// * non-unique column
		rows, err := db.PointQuery(colIndex, val)
		if err != nil {
			return err
		}
		fmt.Println("Rows to delete: ", len(rows))
		for _, row := range rows {
			fmt.Println("Deleting: ", row)
			for i, uniqueColIndex := range db.records.UniqueCols {
				colKeyToDel, _ := checkTypeAndEncodeByte(db.records.TableDef, uniqueColIndex, row[uniqueColIndex], []byte{})
				err := db.uniqueColumnsTree[i].Remove(colKeyToDel)
				if err != nil {
					return err
				}
			}
			pKey, _ := checkTypeAndEncodeByte(db.records.TableDef, 0, row[0], []byte{})
			err := db.records.Remove(pKey)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func (db *DB) Close() {
	for _, uniqueTree := range db.uniqueColumnsTree {
		uniqueTree.Close()
	}
	db.records.Close()
}

func decodeRow(tD *TableDef, buf []byte) []any {
	// fmt.Println("Decoding: ", buf)
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
	// case float64:
	// 	if tD.Types[colIndex] != TYPE_INT64 {
	// 		return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
	// 	}
	// 	buf = utils.AddInt(buf, int(data))
	case []byte:
		if tD.Types[colIndex] != TYPE_BYTE {
			return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
		}
		buf = utils.AddByte(buf, data)
	// case string:
	// 	if tD.Types[colIndex] != TYPE_BYTE {
	// 		return nil, errors.New("[Error]:wrong type for coloumn: " + tD.Cols[colIndex])
	// 	}
	// 	buf = utils.AddByte(buf, []byte(data))
	default:
		fmt.Printf("Type: %T\n", val)
		fmt.Println("Data Type: ", val, data)
		// panic("[Error]:wrong data type passed to function")
		return nil, errors.New("[Error]:wrong data type passed to function")
	}
	return buf, nil
}

func checkUniqueColAndEncode(tD *TableDef, colIndex int, val any) ([]byte, int, error) {
	var collectionIndex int = -1
	for i, col := range tD.UniqueCols {
		if col == colIndex {
			collectionIndex = i
		}
	}
	if collectionIndex == -1 {
		return nil, 0, errors.New("[error] not a unique column")
	}
	key, err := checkTypeAndEncodeByte(tD, colIndex, val, []byte{})
	if err != nil {
		return nil, 0, err
	}
	return key, collectionIndex, err
}
