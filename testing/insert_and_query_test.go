package testing

import (
	"BynxDB/core"
	"strings"
	"testing"
)

// This will allow tests and application code to properly handle different error conditions

func TestInsertAndQueryOnly(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("test", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	// Find an unused ID by checking for existing data
	testID := 1000
	_, err = db.PKeyQuery(testID)
	for err == nil {
		// ID exists, try next one
		testID++
		_, err = db.PKeyQuery(testID)
	}
	t.Logf("Using test ID: %d", testID)

	// Test 1: Insert a new row with unused ID
	err = db.Insert(testID, []byte("Amit"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Test 2: Try to insert duplicate key (should fail as expected)
	err = db.Insert(testID, []byte("Different Name"))
	if err == nil {
		t.Errorf("Expected error when inserting duplicate key, got nil")
	} else if !strings.Contains(err.Error(), "already excists") {
		t.Errorf("Expected duplicate key error, got: %v", err)
	} else {
		t.Logf("Duplicate key correctly rejected: %v", err)
	}

	// Test 3: Query by primary key
	row, err := db.PKeyQuery(testID)
	if err != nil {
		t.Fatalf("PKeyQuery failed: %v", err)
	}
	// Handle both int and int64 types
	var rowID int
	switch v := row[0].(type) {
	case int:
		rowID = v
	case int64:
		rowID = int(v)
	default:
		t.Fatalf("Expected ID to be int or int64, got %T", row[0])
	}
	nameBytes, ok := row[1].([]byte)
	if !ok {
		t.Fatalf("Expected NAME to be []byte, got %T", row[1])
	}
	if rowID != testID || string(nameBytes) != "Amit" {
		t.Errorf("Unexpected row: got [%d %s], expected [%d Amit]", rowID, string(nameBytes), testID)
	}

	// Test 4: Query by column (NAME) - should return all rows with NAME="Amit"
	rows, err := db.PointQuery(1, []byte("Amit"))
	if err != nil {
		t.Fatalf("PointQuery failed: %v", err)
	}
	if len(rows) < 1 {
		t.Errorf("Expected at least 1 row with NAME=Amit, got %d rows", len(rows))
	}
	// Verify our inserted row is in the results
	found := false
	for _, r := range rows {
		// Handle both int and int64 types
		var rowID int
		switch v := r[0].(type) {
		case int:
			rowID = v
		case int64:
			rowID = int(v)
		default:
			continue
		}
		if rowID == testID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find testID %d in query results, got: %v", testID, rows)
	}

	// Test 5: Insert another row with different ID
	testID2 := testID + 1
	err = db.Insert(testID2, []byte("Priya"))
	if err != nil {
		t.Fatalf("Insert second row failed: %v", err)
	}

	// Test 6: Verify both rows exist
	row2, err := db.PKeyQuery(testID2)
	if err != nil {
		t.Fatalf("PKeyQuery for second row failed: %v", err)
	}
	// Handle both int and int64 types
	var rowID2 int
	switch v := row2[0].(type) {
	case int:
		rowID2 = v
	case int64:
		rowID2 = int(v)
	default:
		t.Fatalf("Expected ID to be int or int64, got %T", row2[0])
	}
	nameBytes2, ok := row2[1].([]byte)
	if !ok {
		t.Fatalf("Expected NAME to be []byte, got %T", row2[1])
	}
	if rowID2 != testID2 || string(nameBytes2) != "Priya" {
		t.Errorf("Unexpected second row: got [%d %s], expected [%d Priya]", rowID2, string(nameBytes2), testID2)
	}

	t.Log("All tests passed successfully!")
}
