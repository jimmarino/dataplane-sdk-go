//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

type dbTransactionKeyType struct{}

// DBTransactionKey defines the key for obtaining the transaction from the context.
var DBTransactionKey = dbTransactionKeyType{}

type DBTransactionContext struct {
	db *sql.DB
}

func NewDBTransactionContext(db *sql.DB) *DBTransactionContext {
	return &DBTransactionContext{db: db}
}

func (trxContext *DBTransactionContext) Execute(ctx context.Context, operation func(context.Context) error) error {
	// begin transaction
	tx, err := trxContext.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// rollback on panic
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p) // re-throw panic
		}
	}()

	// execute the operation
	opCtx := context.WithValue(ctx, DBTransactionKey, tx)
	if err := operation(opCtx); err != nil {
		// Rollback on error
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("operation failed: %v, rollback failed: %v", err, rbErr)
		}
		return err
	}

	// commit if no errors
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
