package testing

import (
	"BynxDB/core"
	"fmt"
	"testing"
)

func TestLargeInsertions(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("stress", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	// Insert a large number of records to trigger multiple splits
	numRecords := 100
	t.Logf("Inserting %d records...", numRecords)

	for i := 0; i < numRecords; i++ {
		name := fmt.Sprintf("Person_%d", i)
		err := db.Insert(i, []byte(name))
		if err != nil {
			t.Fatalf("Insert failed for ID %d: %v", i, err)
		}
		if i%20 == 0 {
			t.Logf("Inserted %d records", i)
		}
	}

	t.Log("All inserts completed. Now querying all records...")

	// Query all records to ensure they exist
	missingRecords := []int{}
	for i := 0; i < numRecords; i++ {
		row, err := db.PKeyQuery(i)
		if err != nil {
			t.Errorf("Query failed for ID %d: %v", i, err)
			missingRecords = append(missingRecords, i)
		} else if row == nil {
			t.Errorf("Query returned nil for ID %d", i)
			missingRecords = append(missingRecords, i)
		}
	}

	if len(missingRecords) > 0 {
		t.Errorf("Missing records: %v", missingRecords)
	} else {
		t.Log("All records found successfully!")
	}

	t.Log("Testing duplicate insertions...")

	// Try inserting duplicates - all should fail
	duplicateSuccesses := []int{}
	for i := 0; i < numRecords; i++ {
		err := db.Insert(i, []byte("Duplicate"))
		if err == nil {
			t.Errorf("Expected duplicate key error for ID %d, got nil", i)
			duplicateSuccesses = append(duplicateSuccesses, i)
		}
	}

	if len(duplicateSuccesses) > 0 {
		t.Errorf("Duplicates were inserted for IDs: %v - THIS IS THE BUG!", duplicateSuccesses)

		// Let's verify these duplicates actually exist
		t.Log("Verifying duplicate entries...")
		for _, id := range duplicateSuccesses {
			row, err := db.PKeyQuery(id)
			if err == nil && row != nil {
				t.Logf("ID %d exists in database", id)
			}
		}
	} else {
		t.Log("All duplicates correctly rejected!")
	}
}

func TestRandomInsertDelete(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("random", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	// Insert, then delete some, then insert more
	t.Log("Phase 1: Insert 50 records")
	for i := 0; i < 50; i++ {
		err := db.Insert(i, []byte(fmt.Sprintf("Initial_%d", i)))
		if err != nil {
			t.Fatalf("Initial insert failed for ID %d: %v", i, err)
		}
	}

	t.Log("Phase 2: Delete every other record")
	for i := 0; i < 50; i += 2 {
		err := db.Delete(0, i)
		if err != nil {
			t.Errorf("Delete failed for ID %d: %v", i, err)
		}
	}

	t.Log("Phase 3: Insert new records in the gaps")
	for i := 50; i < 100; i++ {
		err := db.Insert(i, []byte(fmt.Sprintf("New_%d", i)))
		if err != nil {
			t.Fatalf("New insert failed for ID %d: %v", i, err)
		}
	}

	t.Log("Phase 4: Try to re-insert the deleted records")
	for i := 0; i < 50; i += 2 {
		err := db.Insert(i, []byte(fmt.Sprintf("Reinserted_%d", i)))
		if err != nil {
			t.Errorf("Reinsertion failed for deleted ID %d: %v", i, err)
		}
	}

	t.Log("Phase 5: Verify all records exist")
	// Odd numbers from 0-49 (never deleted)
	for i := 1; i < 50; i += 2 {
		row, err := db.PKeyQuery(i)
		if err != nil || row == nil {
			t.Errorf("Missing odd record ID %d", i)
		}
	}

	// Even numbers from 0-49 (deleted then reinserted)
	for i := 0; i < 50; i += 2 {
		row, err := db.PKeyQuery(i)
		if err != nil || row == nil {
			t.Errorf("Missing reinserted record ID %d", i)
		}
	}

	// New records 50-99
	for i := 50; i < 100; i++ {
		row, err := db.PKeyQuery(i)
		if err != nil || row == nil {
			t.Errorf("Missing new record ID %d", i)
		}
	}

	t.Log("Phase 6: Check for duplicate issues")
	for i := 0; i < 100; i++ {
		err := db.Insert(i, []byte("Duplicate"))
		if err == nil {
			t.Errorf("BUG: Duplicate insert succeeded for ID %d", i)
		}
	}
}
