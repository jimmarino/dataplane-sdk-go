package dsdk

import (
	"context"
	"github.com/stretchr/testify/mock"
	"testing"
)

func Test_DataPlaneSDK_Start(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onStart: func(context.Context, *DataFlow, bool, *DataPlaneSDK) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{}, nil
		},
	}

	ctx := context.TODO()
	store.EXPECT().FindById(ctx, "process123").Return(nil, ErrNotFound)
	store.EXPECT().Create(ctx, mock.Anything).Return(nil)
	_, _ = dsdk.Start(ctx, DataFlowStartMessage{DataFlowBaseMessage: DataFlowBaseMessage{
		ProcessId: "process123",
	}})
}

type mockTrxContext struct {
}

func (c *mockTrxContext) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(context.TODO())
}
