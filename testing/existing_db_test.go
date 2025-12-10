package testing

import (
	"BynxDB/core"
	"fmt"
	"testing"
)

func TestExistingDatabase(t *testing.T) {
	// Test 1: Open the existing STRESSrec database which has 100 records
	t.Log("Opening existing STRESSrec database...")

	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}

	db, err := core.DbInit("stress", tDef)
	if err != nil {
		t.Fatalf("Failed to open existing database: %v", err)
	}
	defer db.Close()

	t.Log("Testing queries on existing data...")

	// Query some existing records
	testIDs := []int{0, 10, 26, 53, 80, 99}
	for _, id := range testIDs {
		row, err := db.PKeyQuery(id)
		if err != nil {
			t.Errorf("Query failed for existing ID %d: %v", id, err)
		} else if row == nil {
			t.Errorf("Query returned nil for existing ID %d", id)
		} else {
			t.Logf("Successfully queried ID %d: %v", id, row)
		}
	}

	t.Log("Testing duplicate insertion on existing records...")

	// Try to insert duplicates of existing records
	duplicateAllowed := []int{}
	for _, id := range testIDs {
		err := db.Insert(id, []byte(fmt.Sprintf("Duplicate_%d", id)))
		if err == nil {
			duplicateAllowed = append(duplicateAllowed, id)
			t.Errorf("BUG: Duplicate insert succeeded for existing ID %d", id)
		}
	}

	if len(duplicateAllowed) > 0 {
		t.Errorf("CRITICAL BUG: Duplicates allowed for existing records: %v", duplicateAllowed)
	} else {
		t.Log("All duplicate attempts correctly rejected!")
	}

	t.Log("Testing insertion of new records in existing database...")

	// Insert new records (IDs 100-109)
	newIDs := []int{100, 101, 102, 103, 104, 105, 106, 107, 108, 109}
	for _, id := range newIDs {
		err := db.Insert(id, []byte(fmt.Sprintf("NewRecord_%d", id)))
		if err != nil {
			t.Errorf("Failed to insert new record ID %d: %v", id, err)
		}
	}

	t.Log("Verifying new records were inserted...")

	// Query the new records
	for _, id := range newIDs {
		row, err := db.PKeyQuery(id)
		if err != nil {
			t.Errorf("Query failed for new ID %d: %v", id, err)
		} else if row == nil {
			t.Errorf("New record ID %d not found", id)
		} else {
			t.Logf("Successfully inserted and queried new ID %d", id)
		}
	}

	t.Log("Testing duplicate prevention for newly inserted records...")

	// Try to duplicate the newly inserted records
	for _, id := range newIDs {
		err := db.Insert(id, []byte("Duplicate"))
		if err == nil {
			t.Errorf("BUG: Duplicate insert succeeded for newly inserted ID %d", id)
		}
	}

	t.Log("All tests on existing database completed!")
}

func TestReopenAndVerify(t *testing.T) {
	// Test reopening a database and verifying persistence
	t.Log("Creating a test database...")

	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}

	// First session: Create and insert
	db1, err := core.DbInit("reopen_test", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}

	t.Log("Inserting 20 records in first session...")
	for i := 0; i < 20; i++ {
		err := db1.Insert(i, []byte(fmt.Sprintf("Session1_%d", i)))
		if err != nil {
			t.Fatalf("Insert failed in session 1 for ID %d: %v", i, err)
		}
	}

	db1.Close()
	t.Log("First session closed. Reopening database...")

	// Second session: Reopen and verify
	db2, err := core.DbInit("reopen_test", tDef)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	t.Log("Verifying all 20 records exist after reopening...")
	missingRecords := []int{}
	for i := 0; i < 20; i++ {
		row, err := db2.PKeyQuery(i)
		if err != nil || row == nil {
			missingRecords = append(missingRecords, i)
			t.Errorf("Record ID %d missing after reopen", i)
		}
	}

	if len(missingRecords) > 0 {
		t.Errorf("CRITICAL: Lost records after reopen: %v", missingRecords)
	} else {
		t.Log("All records persisted correctly!")
	}

	t.Log("Testing duplicate prevention after reopen...")
	duplicateAllowed := []int{}
	for i := 0; i < 20; i++ {
		err := db2.Insert(i, []byte("Duplicate"))
		if err == nil {
			duplicateAllowed = append(duplicateAllowed, i)
			t.Errorf("BUG: Duplicate allowed after reopen for ID %d", i)
		}
	}

	if len(duplicateAllowed) > 0 {
		t.Errorf("CRITICAL BUG: Duplicates allowed after reopen: %v", duplicateAllowed)
	} else {
		t.Log("All duplicate attempts correctly rejected after reopen!")
	}

	t.Log("Inserting additional records in second session...")
	for i := 20; i < 30; i++ {
		err := db2.Insert(i, []byte(fmt.Sprintf("Session2_%d", i)))
		if err != nil {
			t.Errorf("Insert failed in session 2 for ID %d: %v", i, err)
		}
	}

	t.Log("Verifying all 30 records now exist...")
	for i := 0; i < 30; i++ {
		row, err := db2.PKeyQuery(i)
		if err != nil || row == nil {
			t.Errorf("Record ID %d missing", i)
		}
	}

	t.Log("Reopen and verify test completed!")
}
