//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	"github.com/jimmarino/dataplane-sdk-go/pkg/dsdk"
)

type PostgresStore struct {
	db *sql.DB
}

func (p PostgresStore) FindById(ctx context.Context, s string) (*dsdk.DataFlow, error) {
	//TODO not implemented
	panic("not implemented")
}

func (p PostgresStore) Create(ctx context.Context, flow *dsdk.DataFlow) error {
	//TODO not implemented
	panic("not implemented")
}

func (p PostgresStore) Save(ctx context.Context, flow *dsdk.DataFlow) error {
	//TODO not implemented
	panic("not implemented")
}

func (p PostgresStore) Delete(ctx context.Context, id string) error {
	//TODO not implemented
	panic("not implemented")
}

func (p PostgresStore) AcquireDataFlowsForRecovery(ctx context.Context) dsdk.Iterator[*dsdk.DataFlow] {
	//TODO not implemented
	panic("not implemented")
}
