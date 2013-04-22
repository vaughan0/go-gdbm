// Package gdbm provides support for the GNU database manager.
package gdbm

/*
#cgo LDFLAGS: -lgdbm
#include <gdbm.h>
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

type Mode int

const (
	// Open as a reader.
	Reader Mode = C.GDBM_READER
	// Open as a writer. The database must already exist.
	Writer = C.GDBM_WRITER
	// Open as a writer and create the database if it does not exist.
	Create = C.GDBM_WRCREAT
	// Open as a writer and create a new database regardless if one already exists.
	ForceCreate = C.GDBM_NEWDB
)

type Database struct {
	dbf C.GDBM_FILE
}

type Config struct {
	// Name of the database file.
	File string
	// Permission bits to use when creating a new database file.
	Perm int
	// How to open the database. See the documentation for the Mode type.
	Mode Mode
	// If true, all database operations will be synchronized to the disk.
	Sync bool
	// If true, no file locking will be performed on the database.
	DontLock bool
	// When creating new databases, BlockSize refers to the size of a single
	// transfer from disk to memory. The minimum value is 512. If BlockSize
	// is zero, then the file system default will be used.
	BlockSize int
	// CacheSize sets the size of the internal bucket cache. If zero, the cache size
	// will be set to 100.
	CacheSize int
}

// Opens a database using the given configuration options.
func OpenConfig(config *Config) (*Database, error) {
	cname := C.CString(config.File)
	defer C.free(unsafe.Pointer(cname))
	mode := C.int(config.Mode)
	if config.Sync {
		mode |= C.GDBM_SYNC
	}
	if config.DontLock {
		mode |= C.GDBM_NOLOCK
	}
	dbf, err := C.gdbm_open(cname, C.int(config.BlockSize), mode, C.int(config.Perm), nil)
	if dbf != nil {
		if config.CacheSize != 0 {
			val := C.int(config.CacheSize)
			C.gdbm_setopt(dbf, C.GDBM_CACHESIZE, &val, C.int(unsafe.Sizeof(val)))
		}
		return &Database{dbf}, nil
	}
	return nil, err
}

// Opens a database with the default options. The database will be
// created if it does not exist (with permission bits 0666, before umask).
func Open(file string) (*Database, error) {
	return OpenConfig(&Config{
		File: file,
		Perm: 0666,
		Mode: Create,
	})
}

// Closes the database and releases all associated resources.
func (d *Database) Close() {
	C.gdbm_close(d.dbf)
}

// Returns the data associated with a given key, or nil if the key is not
// present in the database.
func (d *Database) Fetch(key []byte) (value []byte) {
	dkey := toDatum(key)
	dval := C.gdbm_fetch(d.dbf, dkey)
	if dval.dptr != nil {
		defer C.free(unsafe.Pointer(dval.dptr))
		value = C.GoBytes(unsafe.Pointer(dval.dptr), dval.dsize)
	}
	return
}

// Stores data for a specified key. Any existing data for the key will be replaced.
// If `data` is nil, then the key will be deleted if it exists in the database.
func (d *Database) Store(key, data []byte) {
	dkey := toDatum(key)
	if data == nil {
		C.gdbm_delete(d.dbf, dkey)
	} else {
		C.gdbm_store(d.dbf, dkey, toDatum(data), C.GDBM_REPLACE)
	}
}

// Returns true if the specified key is found in the database.
func (d *Database) Exists(key []byte) bool {
	ret := C.gdbm_exists(d.dbf, toDatum(key))
	return ret != 0
}

// Iterates through all the keys in the database. The callback will be called for
// each key. The callback should return true unless it wants to cancel the iteration.
// Keys will be traversed in an unspecified order.
func (d *Database) Iterate(callback func(key []byte) (cont bool)) {
	key := C.gdbm_firstkey(d.dbf)
	for key.dptr != nil {
		key = d.iterKey(callback, key)
	}
}

func (d *Database) iterKey(callback func([]byte) bool, key C.datum) (next C.datum) {
	defer C.free(unsafe.Pointer(key.dptr))
	bytes := C.GoBytes(unsafe.Pointer(key.dptr), key.dsize)
	if callback(bytes) {
		next = C.gdbm_nextkey(d.dbf, key)
	}
	return
}

// Reorganizes the database file in order to reduce its size by reusing free space.
// This should be used very infrequently, and will only be useful after a lot of deletions
// have been made.
func (d *Database) Reorganize() {
	C.gdbm_reorganize(d.dbf)
}

// Synchronizes the database to disk. Sync will only return once the database has been
// physically written to the disk.
func (d *Database) Sync() {
	C.gdbm_sync(d.dbf)
}

func toDatum(data []byte) C.datum {
	var ptr unsafe.Pointer
	var size int
	if len(data) > 0 {
		ptr = unsafe.Pointer(&data[0])
		size = len(data)
	} else if data != nil {
		// GDBM requires a non-NULL pointer, but the size is zero so it can be arbitrary
		ptr = unsafe.Pointer(uintptr(1))
	}
	return C.datum{
		dptr:  (*C.char)(ptr),
		dsize: C.int(size),
	}
}

func fromDatum(datum C.datum) (bytes []byte) {
	if datum.dptr != nil {
		defer C.free(unsafe.Pointer(datum.dptr))
		bytes = C.GoBytes(unsafe.Pointer(datum.dptr), datum.dsize)
	}
	return
}
