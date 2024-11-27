package utils

import (
	"BynxDB/core"
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

func TableInit() (*core.DB, error) {

	tableName := handleInput("Enter the table name you want to use: ")

	if strings.Contains(tableName, " ") {
		return nil, fmt.Errorf("[error] Invalid Table name")
	}
	pathToTable := "./db/" + strings.ToUpper(tableName) + "rec.db"
	if _, err := os.Stat(pathToTable); errors.Is(err, os.ErrNotExist) {
		tableSchema := handleInput("Table not Found!\nEnter Schema For New Table: ")

		if !strings.ContainsRune(tableSchema, rune('(')) || !strings.ContainsRune(tableSchema, rune(')')) {
			return nil, fmt.Errorf("[error] enclose the columns in parenthesis")
		}
		if !strings.Contains(tableSchema, "PRIMARY") {
			return nil, fmt.Errorf("[error] define a primary key column")
		}
		schema := tableSchema[:]
		schema = strings.ReplaceAll(schema, "  ", " ")
		// cur := 0
		tableDefBuilder := &core.TableDef{}
		parenthesisEndIndex := strings.IndexRune(schema, rune(')'))
		columnsSchema := schema[strings.IndexRune(schema, rune('('))+1 : parenthesisEndIndex]
		schema = schema[parenthesisEndIndex+1:]
		for len(columnsSchema) != 0 {
			commaIndex := strings.IndexRune(columnsSchema, rune(','))
			col := columnsSchema[:]
			if commaIndex != -1 {
				col = columnsSchema[:commaIndex]
			}
			if col[0] == ' ' {
				col = col[1:]
			}
			spaceIndex := strings.IndexRune(col, rune(' '))
			colName := col[:spaceIndex]
			tableDefBuilder.Cols = append(tableDefBuilder.Cols, colName)
			col = col[spaceIndex+1:]
			col = strings.ReplaceAll(col, " ", "")
			switch col {
			case "INT":
				tableDefBuilder.Types = append(tableDefBuilder.Types, core.TYPE_INT64)
			case "BYTE":
				tableDefBuilder.Types = append(tableDefBuilder.Types, core.TYPE_BYTE)
			default:
				return nil, fmt.Errorf("[error] Invliad Data Type")
			}
			if commaIndex == -1 {
				break
			}
			columnsSchema = columnsSchema[commaIndex+1:]
		}
		pKeyIndex := -1
		if schema[0] == ' ' {
			schema = schema[1:]
		}
		schema = schema[len("PRIMARY")+2:]
		pkeyCol := schema[:]
		spaceIndex := strings.IndexRune(pkeyCol, rune(' '))
		if spaceIndex != -1 {
			pkeyCol = pkeyCol[:spaceIndex]

		}
		for i := range tableDefBuilder.Cols {
			if tableDefBuilder.Cols[i] == pkeyCol {
				pKeyIndex = i
			}
		}
		if pKeyIndex == -1 {
			return nil, fmt.Errorf("[error] column not found for primary key")
		}
		tableDefBuilder.PKeyIndex = pKeyIndex

		return core.DbInit(tableName, tableDefBuilder)
	}

	return core.DbInit(tableName, &core.TableDef{})
}

// func Parse(query string) {
// 	queryCond := strings.Split(query, " ")
// 	queryCond = filterEmpty(queryCond)
// 	fmt.Println(queryCond)
// 	fmt.Println(strings.ReplaceAll(query, "  ", ""))
// }

func handleInput(prompt string) string {
	fmt.Print(prompt)

	reader := bufio.NewReader(os.Stdin)

	strToRet, _ := reader.ReadString('\n')
	strToRet = strToRet[:len(strToRet)-1]
	return strToRet
}

func ProcessQueries(db *core.DB) {
	if db != nil {
		fmt.Println("Table loaded Successfully!")
	}
}

func filterEmpty(strArray []string) (retStr []string) {
	for _, str := range strArray {
		if str != "" {
			retStr = append(retStr, str)
		}
	}
	return
}
