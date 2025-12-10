package testing

import (
	"BynxDB/core"
	"testing"
)

func TestBulkInsert(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("bulk", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	// Insert multiple rows to trigger node splits
	names := []string{"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"}

	t.Log("Starting bulk insert...")
	for i := 0; i < 20; i++ {
		name := names[i%len(names)]
		err := db.Insert(i, []byte(name))
		if err != nil {
			t.Fatalf("Insert failed for ID %d: %v", i, err)
		}
		t.Logf("Inserted: ID=%d, NAME=%s", i, name)
	}

	t.Log("All inserts completed successfully!")

	// Now try to query and re-insert some IDs
	t.Log("Testing queries and duplicate inserts...")
	for i := 0; i < 20; i++ {
		row, err := db.PKeyQuery(i)
		if err != nil {
			t.Errorf("Query failed for ID %d: %v", i, err)
		} else {
			t.Logf("Query successful: ID=%d, row=%v", i, row)
		}

		// Try to insert duplicate - should fail
		err = db.Insert(i, []byte("Duplicate"))
		if err == nil {
			t.Errorf("Expected duplicate key error for ID %d, got nil", i)
		} else {
			t.Logf("Duplicate correctly rejected for ID %d", i)
		}
	}
}
