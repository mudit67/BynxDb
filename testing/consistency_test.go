package testing

import (
	"BynxDB/core"
	"bytes"
	"fmt"
	"testing"
)

// TestDeleteInsertConsistency tests for data loss when delete and insert operations are interleaved
func TestDeleteInsertConsistency(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME", "STATUS"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("delete_insert_test", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	t.Run("DeleteThenInsertSameID", func(t *testing.T) {
		// Insert a record
		err := db.Insert(100, []byte("Original"), []byte("active"))
		if err != nil {
			t.Fatalf("Initial insert failed: %v", err)
		}

		// Delete it
		err = db.Delete(0, 100)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify it's deleted
		row, err := db.PKeyQuery(100)
		if row != nil {
			t.Error("Record still exists after deletion")
		}

		// Insert with same ID but different data
		err = db.Insert(100, []byte("NewData"), []byte("inactive"))
		if err != nil {
			t.Fatalf("Re-insert with same ID failed: %v", err)
		}

		// Verify the new data is correct
		row, err = db.PKeyQuery(100)
		if err != nil || row == nil {
			t.Fatal("Failed to query after re-insert")
		}
		if !bytes.Equal(row[1].([]byte), []byte("NewData")) {
			t.Errorf("Expected 'NewData', got '%s'", string(row[1].([]byte)))
		}
	})

	t.Run("InterleavedDeleteInsert", func(t *testing.T) {
		// Insert 10 records
		for i := 200; i < 210; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("User_%d", i)), []byte("active"))
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Delete even IDs
		for i := 200; i < 210; i += 2 {
			err := db.Delete(0, i)
			if err != nil {
				t.Errorf("Delete %d failed: %v", i, err)
			}
		}

		// Verify even IDs are deleted
		for i := 200; i < 210; i += 2 {
			row, _ := db.PKeyQuery(i)
			if row != nil {
				t.Errorf("Record %d should be deleted but still exists", i)
			}
		}

		// Verify odd IDs still exist
		for i := 201; i < 210; i += 2 {
			row, err := db.PKeyQuery(i)
			if err != nil || row == nil {
				t.Errorf("Record %d should exist but not found", i)
			}
		}

		// Re-insert even IDs with different data
		for i := 200; i < 210; i += 2 {
			err := db.Insert(i, []byte(fmt.Sprintf("NewUser_%d", i)), []byte("inactive"))
			if err != nil {
				t.Errorf("Re-insert %d failed: %v", i, err)
			}
		}

		// Final verification - all should exist with correct data
		for i := 200; i < 210; i++ {
			row, err := db.PKeyQuery(i)
			if err != nil || row == nil {
				t.Errorf("Final check: Record %d not found", i)
				continue
			}

			// Check data correctness
			if i%2 == 0 {
				expected := fmt.Sprintf("NewUser_%d", i)
				if !bytes.Equal(row[1].([]byte), []byte(expected)) {
					t.Errorf("Record %d: expected '%s', got '%s'", i, expected, string(row[1].([]byte)))
				}
			}
		}
	})

	t.Run("DeleteDuringNodeSplit", func(t *testing.T) {
		// Insert enough records to cause node splits
		for i := 300; i < 330; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Data_%d", i)), []byte("active"))
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Delete records in the middle of the range
		for i := 310; i < 320; i++ {
			err := db.Delete(0, i)
			if err != nil {
				t.Errorf("Delete %d failed: %v", i, err)
			}
		}

		// Verify deletions
		for i := 310; i < 320; i++ {
			row, _ := db.PKeyQuery(i)
			if row != nil {
				t.Errorf("Record %d should be deleted but found", i)
			}
		}

		// Verify remaining records are intact
		for i := 300; i < 310; i++ {
			row, err := db.PKeyQuery(i)
			if err != nil || row == nil {
				t.Errorf("Record %d should exist but not found", i)
			}
		}
		for i := 320; i < 330; i++ {
			row, err := db.PKeyQuery(i)
			if err != nil || row == nil {
				t.Errorf("Record %d should exist but not found", i)
			}
		}
	})
}

// TestUpdateDeleteConsistency tests for data loss when update and delete operations are mixed
func TestUpdateDeleteConsistency(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "VALUE", "STATUS"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("update_delete_test", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	t.Run("UpdateThenDelete", func(t *testing.T) {
		// Insert records
		for i := 1; i <= 5; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Val_%d", i)), []byte("active"))
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Update record 3
		err = db.UpdatePoint(1, []byte("Val_3"), []byte("Updated_3"))
		if err != nil {
			t.Errorf("Update failed: %v", err)
		}

		// Verify update worked
		row, err := db.PKeyQuery(3)
		if err != nil || row == nil {
			t.Fatal("Failed to query after update")
		}
		if !bytes.Equal(row[1].([]byte), []byte("Updated_3")) {
			t.Errorf("Update not reflected: got '%s'", string(row[1].([]byte)))
		}

		// Delete updated record
		err = db.Delete(0, 3)
		if err != nil {
			t.Errorf("Delete after update failed: %v", err)
		}

		// Verify deletion
		row, _ = db.PKeyQuery(3)
		if row != nil {
			t.Error("Record should be deleted after update")
		}

		// Verify other records are unaffected
		for i := 1; i <= 5; i++ {
			if i == 3 {
				continue
			}
			row, err := db.PKeyQuery(i)
			if err != nil || row == nil {
				t.Errorf("Record %d should still exist", i)
			}
		}
	})

	t.Run("DeleteThenUpdate", func(t *testing.T) {
		// Insert records
		for i := 10; i <= 15; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Val_%d", i)), []byte("active"))
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Delete record 12
		err = db.Delete(0, 12)
		if err != nil {
			t.Errorf("Delete failed: %v", err)
		}

		// Try to update deleted record (should fail or have no effect)
		err = db.UpdatePoint(1, []byte("Val_12"), []byte("ShouldNotWork"))
		// This should either error or not find anything to update

		// Verify record is still deleted
		row, _ := db.PKeyQuery(12)
		if row != nil {
			t.Error("Deleted record should not exist")
		}

		// Update a different record to ensure updates still work
		err = db.UpdatePoint(1, []byte("Val_13"), []byte("Updated_13"))
		if err != nil {
			t.Errorf("Update of existing record failed: %v", err)
		}

		row, err = db.PKeyQuery(13)
		if err != nil || row == nil {
			t.Fatal("Failed to query after update")
		}
		if !bytes.Equal(row[1].([]byte), []byte("Updated_13")) {
			t.Errorf("Update not applied correctly")
		}
	})

	t.Run("MassUpdateDelete", func(t *testing.T) {
		// Insert 20 records
		for i := 100; i < 120; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Data_%d", i)), []byte("pending"))
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Update each record individually from "pending" to "active"
		for i := 100; i < 120; i++ {
			row, _ := db.PKeyQuery(i)
			if row != nil {
				err = db.UpdatePoint(2, []byte("pending"), []byte("active"))
				if err != nil {
					// After first update, "pending" won't be found anymore
					// This is expected behavior
				}
			}
		}

		// Delete every third record
		for i := 100; i < 120; i += 3 {
			err := db.Delete(0, i)
			if err != nil {
				t.Errorf("Delete %d failed: %v", i, err)
			}
		}

		// Verify deletions
		deletedCount := 0
		existingCount := 0
		for i := 100; i < 120; i++ {
			row, _ := db.PKeyQuery(i)
			if i%3 == 0 {
				if row != nil {
					t.Errorf("Record %d should be deleted", i)
				} else {
					deletedCount++
				}
			} else {
				if row == nil {
					t.Errorf("Record %d should exist", i)
				} else {
					existingCount++
					// Verify status is still "active"
					if !bytes.Equal(row[2].([]byte), []byte("active")) {
						t.Errorf("Record %d status should be 'active', got '%s'", i, string(row[2].([]byte)))
					}
				}
			}
		}

		t.Logf("Deleted: %d, Existing: %d", deletedCount, existingCount)
	})
}

// TestComplexOperationSequence tests complex sequences that might expose consistency issues
func TestComplexOperationSequence(t *testing.T) {
	tDef := &core.TableDef{
		Cols:       []string{"ID", "NAME"},
		Types:      []uint16{core.TYPE_INT64, core.TYPE_BYTE},
		UniqueCols: []int{0},
	}
	db, err := core.DbInit("complex_ops_test", tDef)
	if err != nil {
		t.Fatalf("DbInit failed: %v", err)
	}
	defer db.Close()

	t.Run("InsertDeleteInsertSequence", func(t *testing.T) {
		// Test rapid insert-delete-insert cycles
		for cycle := 0; cycle < 5; cycle++ {
			// Insert
			err := db.Insert(1000, []byte(fmt.Sprintf("Cycle_%d", cycle)))
			if err != nil {
				t.Fatalf("Cycle %d: Insert failed: %v", cycle, err)
			}

			// Verify inserted
			row, err := db.PKeyQuery(1000)
			if err != nil || row == nil {
				t.Fatalf("Cycle %d: Record not found after insert", cycle)
			}

			// Delete
			err = db.Delete(0, 1000)
			if err != nil {
				t.Fatalf("Cycle %d: Delete failed: %v", cycle, err)
			}

			// Verify deleted
			row, _ = db.PKeyQuery(1000)
			if row != nil {
				t.Fatalf("Cycle %d: Record still exists after delete", cycle)
			}
		}
	})

	t.Run("UpdateNonExistentAfterDelete", func(t *testing.T) {
		// Insert a record
		err := db.Insert(2000, []byte("ToDelete"))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Delete it
		err = db.Delete(0, 2000)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Try to update it (should fail gracefully)
		err = db.UpdatePoint(1, []byte("ToDelete"), []byte("Updated"))
		// Update should either error or do nothing

		// Verify record is still deleted
		row, _ := db.PKeyQuery(2000)
		if row != nil {
			t.Error("Record should remain deleted")
		}
	})

	t.Run("InterleavedOperationsStressTest", func(t *testing.T) {
		// Insert initial batch
		for i := 3000; i < 3050; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Initial_%d", i)))
			if err != nil {
				t.Fatalf("Initial insert %d failed: %v", i, err)
			}
		}

		// Perform interleaved operations
		operations := []struct {
			op   string
			id   int
			data string
		}{
			{"delete", 3010, ""},
			{"insert", 3051, "New_3051"},
			{"update", 3020, "Updated_3020"},
			{"delete", 3030, ""},
			{"insert", 3052, "New_3052"},
			{"delete", 3040, ""},
			{"update", 3025, "Updated_3025"},
			{"insert", 3010, "Reinsert_3010"}, // Reinsert deleted ID
		}

		for _, op := range operations {
			switch op.op {
			case "delete":
				err := db.Delete(0, op.id)
				if err != nil {
					t.Errorf("Delete %d failed: %v", op.id, err)
				}
			case "insert":
				err := db.Insert(op.id, []byte(op.data))
				if err != nil {
					t.Errorf("Insert %d failed: %v", op.id, err)
				}
			case "update":
				// Find current value first
				row, _ := db.PKeyQuery(op.id)
				if row != nil {
					err := db.UpdatePoint(1, row[1], []byte(op.data))
					if err != nil {
						t.Errorf("Update %d failed: %v", op.id, err)
					}
				}
			}
		}

		// Verify final state
		// 3010 should exist (reinserted)
		row, err := db.PKeyQuery(3010)
		if err != nil || row == nil {
			t.Error("3010 should exist after reinsertion")
		} else if !bytes.Equal(row[1].([]byte), []byte("Reinsert_3010")) {
			t.Errorf("3010 has wrong data: %s", string(row[1].([]byte)))
		}

		// 3030, 3040 should not exist
		row, _ = db.PKeyQuery(3030)
		if row != nil {
			t.Error("3030 should be deleted")
		}
		row, _ = db.PKeyQuery(3040)
		if row != nil {
			t.Error("3040 should be deleted")
		}

		// 3020, 3025 should exist with updated data
		row, err = db.PKeyQuery(3020)
		if err != nil || row == nil {
			t.Error("3020 should exist")
		}
		row, err = db.PKeyQuery(3025)
		if err != nil || row == nil {
			t.Error("3025 should exist")
		}

		// 3051, 3052 should exist
		row, err = db.PKeyQuery(3051)
		if err != nil || row == nil {
			t.Error("3051 should exist")
		}
		row, err = db.PKeyQuery(3052)
		if err != nil || row == nil {
			t.Error("3052 should exist")
		}
	})

	t.Run("CountRecordsAfterMixedOps", func(t *testing.T) {
		// Insert 30 records
		for i := 4000; i < 4030; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Count_%d", i)))
			if err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Delete 10 records
		for i := 4000; i < 4010; i++ {
			err := db.Delete(0, i)
			if err != nil {
				t.Errorf("Delete %d failed: %v", i, err)
			}
		}

		// Insert 5 new records
		for i := 4030; i < 4035; i++ {
			err := db.Insert(i, []byte(fmt.Sprintf("Count_%d", i)))
			if err != nil {
				t.Errorf("Insert %d failed: %v", i, err)
			}
		}

		// Count existing records - should be 25 (30 - 10 + 5)
		count := 0
		for i := 4000; i < 4035; i++ {
			row, _ := db.PKeyQuery(i)
			if row != nil {
				count++
			}
		}

		expectedCount := 25
		if count != expectedCount {
			t.Errorf("Expected %d records, found %d", expectedCount, count)
		}
	})
}
