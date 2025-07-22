package dsdk

import (
	"context"
	"github.com/stretchr/testify/mock"
	"net/url"
	"testing"
)

func Test_DataPlaneSDK_Start(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{State: Started}, nil
		},
	}

	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(nil, ErrNotFound)
	store.EXPECT().Create(ctx, mock.Anything).Return(nil)

	callbackURL, _ := url.Parse("http://test.com/callback")

	_, _ = dsdk.Start(ctx, DataFlowStartMessage{DataFlowBaseMessage: DataFlowBaseMessage{
		ProcessId:        "process123",
		AgreementId:      "agreement123",
		DatasetId:        "dataset123",
		ParticipantId:    "participant123",
		DataspaceContext: "dscontext",
		CounterPartyId:   "counterparty123",
		CallbackAddress:  CallbackURL(*callbackURL),
		TransferType:     TransferType{DestinationType: "test", FlowType: Pull},
	}})
}

type mockTrxContext struct {
}

func (c *mockTrxContext) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(context.TODO())
}
