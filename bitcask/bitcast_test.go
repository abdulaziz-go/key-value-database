package bitcask

import (
	"testing"
)

func TestBitCaskCRUD(t *testing.T) {
	bc, err := Open("bitacsk.log")
	if err != nil {
		t.Fatalf("BitCask ochishda xatolik: %v", err)
	}
	defer bc.Close()

	t.Run("Create", func(t *testing.T) {
		key := "user:1"
		value := "Alice"
		if err := bc.Put(key, []byte(value)); err != nil {
			t.Fatalf("Put xatolik: %v", err)
		}
		if _, err = bc.Get(key); err != nil && err.Error() == "key not found" {
			t.Fatalf("key topimlmadi")
		}
	})

	t.Run("Read", func(t *testing.T) {
		key := "user:1"
		expected := "Alice"
		value, err := bc.Get(key)
		if err != nil {
			t.Fatalf("Get xatolik: %v", err)
		}
		if string(value) != expected {
			t.Errorf("Noto'g'ri qiymat: kutilgan '%s', olindi '%s'", expected, string(value))
		}
	})

	t.Run("Delete", func(t *testing.T) {
		key := "user:1"
		if err := bc.Delete(key); err != nil {
			t.Fatalf("Delete xatolik: %v", err)
		}
		if _, err = bc.Get(key); err == nil {
			t.Fatalf("key o'chirlmagan")
		}
	})
}
