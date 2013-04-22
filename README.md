go-gdbm
=======

go-gdbm provides Go bindings for the [GNU database manager](http://www.gnu.org.ua/software/gdbm/).
The go-gdbm project is licensed under the MIT license.

You can view the API documentation [here](http://godoc.org/github.com/vaughan0/go-gdbm).

Basic Usage
-----------

Open a database:

```go
db, err := gdbm.Open("my-stuff.db")
if err != nil {
  panic(err)
}
defer db.Close()
```

Store some data:

```go
db.Store([]byte("strawberry"), []byte("red"))
db.Store([]byte("banana"), []byte("yellow"))
```

To delete an entry, call Store with nil as the second parameter.

To retrieve data for a key:

```go
value := db.Fetch([]byte("strawberry"))
if string(value) != "red" {
  panic("oh no!")
}
```
