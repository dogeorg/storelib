package storelib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/mattn/go-sqlite3"
)

var ErrNotFound = errors.New("not-found")
var ErrAlreadyExists = errors.New("already-exists")
var ErrConflict = errors.New("db-conflict")

// StoreBase must be included in your Store imeplentation struct
type StoreBase[Store any, StoreTx any] struct {
	TX         Queryable // Query the DB inside/outside a Transaction
	RawDB      *sql.DB   // Bypass TX and directly access the DB
	Ctx        context.Context
	isPostgres bool
	impl       StoreImpl[Store, StoreTx]
}

type StoreImpl[Store any, StoreTx any] interface {
	// Clone must make a new copy of the Store implementation,
	// returning the new copy and a pointer to the StoreBase inside the new copy.
	Clone() (StoreImpl[Store, StoreTx], *StoreBase[Store, StoreTx], Store, StoreTx)
}

var _ StoreAPI[any, any] = &StoreBase[any, any]{} // interface assertion

// StoreAPI must be included in your Store interface,
// along with your public Store methods.
type StoreAPI[Store any, StoreTx any] interface {

	// Transact performs a transactional update function
	Transact(fn func(tx StoreTx) error) error

	// WithCtx returns the same Store interface, bound to a specific cancellable Context
	WithCtx(ctx context.Context) Store

	// Close closes the database.
	Close()
}

// The common read-only parts of sql.DB and sql.Tx interfaces
type Queryable interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

const VERSION_SCHEMA string = `
CREATE TABLE IF NOT EXISTS migration (
	version INTEGER NOT NULL DEFAULT 1
);
`

// Migration is a schema migration to pass to InitStoreLib
type Migration struct {
	Version int           // Monotonically increasing schema version
	SQL     string        // Query to uprade the schema
	Update  FnInitAppData // Callback to run after the migration (can be nil)
}

// FnInitAppData is an optional callback to run after the migration,
// within the same transaction.
type FnInitAppData func(*sql.Tx) error

// InitStore initialises a Store that uses Postgres or SQLite
func InitStore[Store any, StoreTx any](impl StoreImpl[Store, StoreTx], base *StoreBase[Store, StoreTx], connString string, migrations []Migration, ctx context.Context) error {
	backend := "sqlite3"
	if strings.HasPrefix(connString, "postgres://") {
		// "postgres://user:password@host/dbname"
		backend = "postgres"
		base.isPostgres = true
	}
	db, err := sql.Open(backend, connString)
	if err != nil {
		return base.DBErr(err, "opening database")
	}
	base.RawDB = db
	base.TX = db // initial `tx` is the database itself.
	base.Ctx = ctx
	base.impl = impl
	if backend == "sqlite3" {
		// limit concurrent access until we figure out a way to start transactions
		// with the BEGIN CONCURRENT statement in Go. Avoids "database locked" errors.
		db.SetMaxOpenConns(1)
	}
	err = base.applyMigrations(migrations)
	return err
}

func (s *StoreBase[Store, StoreTx]) applyMigrations(migrations []Migration) error {
	var version int
	err := s.doTxn("version schema", func(tx *sql.Tx) error {
		// apply migrations
		verRow := tx.QueryRow("SELECT version FROM migration LIMIT 1")
		err := verRow.Scan(&version)
		if err != nil {
			// first-time database init (idempotent)
			_, err := tx.Exec(VERSION_SCHEMA)
			if err != nil {
				return s.DBErr(err, "creating database schema")
			}
			// set up version table (idempotent)
			err = tx.QueryRow("SELECT version FROM migration LIMIT 1").Scan(&version)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					version = 1
					_, err = tx.Exec("INSERT INTO migration (version) VALUES (?)", version)
					if err != nil {
						return s.DBErr(err, "updating version")
					}
				} else {
					return s.DBErr(err, "querying version")
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	priorVer := -1
	for _, m := range migrations {
		// detect duplicate and out of order migrations
		if m.Version <= priorVer {
			panic(fmt.Sprintf("malformed migrations: version %v is <= prior version %v", m.Version, priorVer))
		}
		priorVer = m.Version
		if version < m.Version {
			name := fmt.Sprintf("migration %v", m.Version)
			err = s.doTxn(name, func(tx *sql.Tx) error {
				_, err = tx.Exec(m.SQL)
				if err != nil {
					return s.DBErr(err, fmt.Sprintf("applying migration %v", m.Version))
				}
				_, err = tx.Exec("UPDATE migration SET version=?", version)
				if err != nil {
					return s.DBErr(err, "updating version")
				}
				if m.Update != nil {
					err = m.Update(tx)
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
			version = m.Version
		}
	}
	return nil
}

// Close closes the connection to the store for clean shutdown.
func (s *StoreBase[Store, StoreTx]) Close() {
	s.RawDB.Close()
}

// WithCtx returns the same Store interface, bound to a specific cancellable Context.
func (s *StoreBase[Store, StoreTx]) WithCtx(ctx context.Context) Store {
	impl, base, store, _ := s.impl.Clone()
	if impl == s.impl || base == s {
		panic("Clone() must return a new instance (it returned the pointer passed in)")
	}
	// copy our base fields, changing 'ctx' and 'impl'
	base.RawDB = s.RawDB
	base.TX = s.TX
	base.Ctx = ctx
	base.isPostgres = s.isPostgres
	base.impl = impl
	return store
}

// Transact runs `fn` inside a transaction, retrying on DB conflicts.
func (s *StoreBase[Store, StoreTx]) Transact(fn func(tx StoreTx) error) error {
	return s.doTxn("Transact", func(tx *sql.Tx) error {
		impl, base, _, storeTx := s.impl.Clone()
		if impl == s.impl || base == s {
			panic("Clone() must return a new instance (it returned the pointer passed in)")
		}
		// copy our base fields, changing 'tx' and 'impl'
		base.RawDB = s.RawDB
		base.TX = tx
		base.Ctx = s.Ctx
		base.isPostgres = s.isPostgres
		base.impl = impl
		return fn(storeTx)
	})
}

func (s StoreBase[Store, StoreTx]) doTxn(name string, work func(tx *sql.Tx) error) error {
	limit := 120
	for {
		tx, err := s.RawDB.Begin()
		if err != nil {
			return s.DBErr(err, "cannot begin transaction: "+name)
		}
		defer tx.Rollback()
		err = work(tx)
		if err != nil {
			if s.IsConflict(err) {
				limit--
				if limit != 0 {
					s.sleep(250 * time.Millisecond)
					continue // try again
				}
			}
			// roll back and return error
			return err
		}
		err = tx.Commit()
		if err != nil {
			if s.IsConflict(err) {
				limit--
				if limit != 0 {
					s.sleep(250 * time.Millisecond)
					continue // try again
				}
			}
			// roll back and return error
			return s.DBErr(err, "cannot commit: "+name)
		}
		// success
		return nil
	}
}

func (s StoreBase[Store, StoreTx]) sleep(dur time.Duration) {
	select {
	case <-s.Ctx.Done():
	case <-time.After(dur):
	}
}

func (s *StoreBase[Store, StoreTx]) IsConflict(err error) bool {
	if s.isPostgres {
		if pqErr, isPq := err.(*pq.Error); isPq {
			name := pqErr.Code.Name()
			if name == "serialization_failure" || name == "transaction_integrity_constraint_violation" {
				// Transaction rollback due to serialization conflict.
				// Transient database conflict: the caller should retry.
				return true
			}
		}
	} else {
		if sqErr, isSq := err.(sqlite3.Error); isSq {
			if sqErr.Code == sqlite3.ErrBusy || sqErr.Code == sqlite3.ErrLocked {
				// SQLite has a single-writer policy, even in WAL (write-ahead) mode.
				// SQLite will return BUSY if the database is locked by another connection.
				// We treat this as a transient database conflict, and the caller should retry.
				return true
			}
		}
	}
	return false
}

func (s StoreBase[Store, StoreTx]) DBErr(err error, where string) error {
	// pass through ErrNotFound if returned from a Transaction
	if errors.Is(err, ErrNotFound) {
		return err
	}
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	if s.isPostgres {
		if pqErr, isPq := err.(*pq.Error); isPq {
			name := pqErr.Code.Name()
			if name == "unique_violation" {
				// MUST detect 'AlreadyExists' to fulfil the API contract!
				return ErrAlreadyExists
			}
			if name == "serialization_failure" || name == "transaction_integrity_constraint_violation" {
				// Transaction rollback due to serialization conflict.
				// Transient database conflict: the caller should retry.
				return ErrConflict
			}
		}
		return fmt.Errorf("PostgresStore: %w", err) // 'wraps' the error
	} else {
		if sqErr, isSq := err.(sqlite3.Error); isSq {
			if sqErr.Code == sqlite3.ErrConstraint {
				// MUST detect 'AlreadyExists' to fulfil the API contract!
				// Constraint violation, e.g. a duplicate key.
				return ErrAlreadyExists
			}
			if sqErr.Code == sqlite3.ErrBusy || sqErr.Code == sqlite3.ErrLocked {
				// SQLite has a single-writer policy, even in WAL (write-ahead) mode.
				// SQLite will return BUSY if the database is locked by another connection.
				// We treat this as a transient database conflict, and the caller should retry.
				return ErrConflict
			}
		}
		return fmt.Errorf("SQLiteStore: %w", err) // 'wraps' the error
	}
}
