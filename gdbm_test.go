package gdbm

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func mockDb(f func(*Database)) {
	tmp, err := ioutil.TempFile("", "go-gdbm-test_")
	if err != nil {
		panic(err)
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	db, err := Open(tmp.Name())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	f(db)
}

func TestOpenDatabase(t *testing.T) {
	mockDb(func(_ *Database) {})
}

func TestOpenFail(t *testing.T) {
	dir, err := ioutil.TempDir("", "go-gdbm-test_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dir)

	file := filepath.Join(dir, "doesnt-exist")
	db, err := OpenConfig(&Config{
		File: file,
		Mode: Writer,
	})

	if db != nil {
		db.Close()
		os.Remove(file)
		t.Error("non-nil Database returned")
	} else if err == nil {
		t.Error("nil Database and nil error returned")
	} else if !os.IsNotExist(err) {
		t.Error("unexpected error:", err)
	}
}

func TestFetchNotPresent(t *testing.T) {
	mockDb(func(d *Database) {
		value := d.Fetch([]byte{1, 2, 3})
		if value != nil {
			t.Errorf("expected nil but got %v", value)
		}
	})
}

func TestStore(t *testing.T) {
	mockDb(func(d *Database) {
		expect := []byte{4, 2}
		d.Store([]byte{1, 2, 3}, expect)
		if value := d.Fetch([]byte{8, 7}); value != nil {
			t.Errorf("expected nil for non-existent key, but got %v", value)
		}
		value := d.Fetch([]byte{1, 2, 3})
		if value == nil {
			t.Errorf("expected %v but got nil", expect)
		} else if !reflect.DeepEqual(value, expect) {
			t.Errorf("expected %v but got %v", expect, value)
		}
	})
}

func TestEmptySlices(t *testing.T) {
	mockDb(func(d *Database) {
		hello := []byte("Hello World!")
		empty := []byte{}
		d.Store(empty, hello)
		if v := d.Fetch(empty); !reflect.DeepEqual(v, hello) {
			t.Errorf("expected %v but got %v", hello, v)
		}
		d.Store(hello, empty)
		if v := d.Fetch(hello); !reflect.DeepEqual(v, empty) {
			t.Errorf("expected %v but got %v", empty, v)
		}
	})
}

func TestExists(t *testing.T) {
	mockDb(func(d *Database) {
		key := []byte("thekey")
		d.Store(key, []byte("lol"))
		if !d.Exists(key) {
			t.Fail()
		}
		if d.Exists([]byte("nope")) {
			t.Fail()
		}
	})
}

func TestDelete(t *testing.T) {
	mockDb(func(d *Database) {
		key, value := []byte{1, 2, 3}, []byte{4, 5, 6}
		d.Store(key, value)
		if v := d.Fetch(key); !reflect.DeepEqual(value, v) {
			t.Errorf("expected %v but got %v", value, v)
		}
		d.Store(key, nil)
		if v := d.Fetch(key); v != nil {
			t.Errorf("expected nil but got %v")
		}
	})
}

func TestIterate(t *testing.T) {
	mockDb(func(d *Database) {
		keys := []string{"gdbm", "has", "a", "nice", "API"}
		for _, key := range keys {
			d.Store([]byte(key), []byte{23, 42})
		}
		present := make(map[string]bool)
		d.Iterate(func(key []byte) bool {
			skey := string(key)
			if present[skey] {
				t.Errorf("key %v already present", key)
			}
			present[skey] = true
			return true
		})
		for _, key := range keys {
			if !present[string(key)] {
				t.Errorf("key %q not handled by Iterate", string(key))
			}
		}
	})
}

func TestIterateCancel(t *testing.T) {
	mockDb(func(d *Database) {
		for i := 0; i < 10; i++ {
			d.Store([]byte{byte(i)}, []byte{23, 42})
		}
		n := 0
		d.Iterate(func(key []byte) bool {
			n++
			return n < 2
		})
		if n != 2 {
			t.Fail()
		}
	})
}

func populate(d *Database, n int) {
	for i := 0; i < n; i++ {
		key := []byte(strconv.Itoa(i))
		d.Store(key, key)
	}
}

func BenchmarkFetch(b *testing.B) {
	const maxKey = 10000
	mockDb(func(d *Database) {
		populate(d, maxKey)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(strconv.Itoa(rand.Intn(maxKey)))
			data := d.Fetch(key)
			if !reflect.DeepEqual(key, data) {
				b.Fatalf("incorrect value for %v: %v", key, data)
			}
		}
	})
}

func BenchmarkReplace(b *testing.B) {
	const maxKey = 10000
	mockDb(func(d *Database) {
		populate(d, maxKey)
		b.ResetTimer()
		value := maxKey + 1
		for i := 0; i < b.N; i++ {
			key := []byte(strconv.Itoa(rand.Intn(maxKey)))
			data := []byte(strconv.Itoa(value))
			value++
			d.Store(key, data)
		}
	})
}

func BenchmarkStore(b *testing.B) {
	mockDb(func(d *Database) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(strconv.Itoa(i))
			d.Store(key, key)
		}
	})
}
