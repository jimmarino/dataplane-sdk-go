package dsdk

import (
	"context"
	"errors"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrInvalidInput = errors.New("invalid input")
	ErrConflict     = errors.New("conflict")
)

// DataplaneStore defines the extension point for finding, creating, saving, and iterating over DataFlow entities.
type DataplaneStore interface {
	// FindById returns a DataFlow for the given id or an error.
	FindById(context.Context, string) (*DataFlow, error)
	Create(context.Context, *DataFlow) error
	Save(context.Context, *DataFlow) error
	Delete(ctx context.Context, id string) error
	AcquireDataFlowsForRecovery(ctx context.Context) Iterator[*DataFlow]
}

// TransactionContext defines an extension point for executing operations within a transactional context.
type TransactionContext interface {
	Execute(func(ctx context.Context) error) error
}

// Iterator for store operations
type Iterator[T any] interface {
	// Next advances the iterator and returns true if there is a next element
	Next() bool

	// Get returns the current element
	Get() T

	// Error returns any error that occurred during iteration
	Error() error

	// Close Releases resources associated with the iterator
	Close() error
}
