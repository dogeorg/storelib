# StoreLib

StoreLib provides a multi-database Store implementation, which currently
supports Postgres and SQLite backends.

## Using

Add storelib to your project:

```
go get github.com/dogeorg/storelib
```

## Example

Use the following template to start your Store implementation.
The comments here will guide your changes:

```go
package store

import (
	"context"

	"github.com/dogeorg/my-new-project/spec" // import your 'spec' package, see below
	"github.com/dogeorg/storelib"
)


// First some type aliases,

// These interfaces define your Store API
type Store = spec.Store
type StoreTx = spec.StoreTx

// These make the code readable
type StoreBase = storelib.StoreBase[Store, StoreTx]
type StoreImpl = storelib.StoreImpl[Store, StoreTx]


// Now define the store,

type MyStore struct {
	StoreBase // pull in the storelib

    // add any fields you need here
}

var _ Store = &MyStore{} // interface assertion


// Create function,

func NewStore(connString string, ctx context.Context) (Store, error) {
	store := &MyStore{}
    // fails if connString is bad, or if schema (migrations) are bad.
	err := storelib.InitStore(store, &store.StoreBase, connString, MIGRATIONS, ctx)
    // initialise your custom fields here (or above)
	return store, err
}


// Clone function, to make transactions work,

func (s *MyStore) Clone() (StoreImpl, *StoreBase, Store, StoreTx) {
	newstore := &MyStore{}
    // copy your custom fields here
	return newstore, &newstore.StoreBase, newstore, newstore
}


// DATABASE SCHEMA

const SCHEMA_v1 = `

    CREATE TABLE ...

`

var MIGRATIONS = []storelib.Migration{
	{Version: 1, SQL: SCHEMA_v1},

    // add DB migrations here as your schema changes
}


// STORE METHODS

// Implement your Store API methods here.
// They will run inside a transaction if called inside the Transact() method,
// otherwise they will run outside of a transaction.
```

Your Store API interfaces also need to mention the storelib:

```go
package spec

import (
	"github.com/dogeorg/storelib"
)

// Methods you can use inside a Transaction
type StoreTx interface {

	// Declare your Store API methods here

}

// Methods on the main Store
type Store interface {
	storelib.StoreAPI[Store, StoreTx] // include the Base Store API
	StoreTx                           // include all the StoreTx methods

    // By including `StoreTx` here, the StoreTx methods are also callable
    // directly on the Store outside a transaction. It's optional.

    // Declare any other Store API methods here.
}

```

The `storelib.StoreAPI` adds the following methods to the Store:

```go
	// Transact performs a transactional update (with retries on conflict)
	Transact(fn func(tx StoreTx) error) error

	// WithCtx returns a new Store, bound to a specific cancellable Context
	WithCtx(ctx context.Context) Store

	// Close closes the database.
	Close()
```

Transact clones the Store so it can swap out the `TX` field, which allows
your method implementations to work both inside and outside a transaction.

All of your Store implementation methods should use `s.TX.Query` and `s.T.QueryRow`
so they will work both inside and outside a transaction.


## Example Store Method

```go
func (s *MyStore) GetFooBarBaz(name string) (id int64, bar string, err error) {
	row := s.TX.QueryRow("SELECT id,bar FROM fooz WHERE name=$1", name)
	err = row.Scan(&id, &bar)
	if err != nil {
		// this will return ErrNotFound or use the error message
		return 0, "", s.DBErr(err, "GetFooBarBaz: error scanning row")
	}
	return
}
```

The `$1` style placeholders are compatible with both Postgres and SQLite.
