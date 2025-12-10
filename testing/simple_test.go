package testing

import (
	"BynxDB/core"
	"testing"
)

func TestSimpleInsert(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("simple", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	// Test with int
	err = db.Insert(1, []byte("Alice"))
	if err != nil {
		t.Errorf("Insert with int failed: %v", err)
	}

	// Test with int64
	err = db.Insert(int64(2), []byte("Bob"))
	if err != nil {
		t.Errorf("Insert with int64 failed: %v", err)
	}

	t.Log("Both int and int64 worked!")
}
