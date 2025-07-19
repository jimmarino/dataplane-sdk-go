package dsdk

import (
	"context"
	"errors"
	"fmt"
)

// DataFlowProcessor is an extension point for handling SDK data flow events. Implementations may modify the data flow instance
// which will be persisted by the SDK.
type DataFlowProcessor func(context.Context, *DataFlow) (*DataFlowResponseMessage, error)

type DataFlowHandler func(context.Context, *DataFlow) error

type DataPlaneSDK struct {
	Store      DataplaneStore
	TrxContext TransactionContext

	OnPrepare   DataFlowProcessor
	OnStart     DataFlowProcessor
	OnTerminate DataFlowHandler
	OnSuspend   DataFlowHandler
	OnRecover   DataFlowHandler
}

// Prepare is called on the consumer to prepare for receiving data.
// It invokes the OnPrepare callback and persists the created flow. Returns a response or an error if the process fails.
func (dsdk *DataPlaneSDK) Prepare(ctx context.Context, message DataFlowPrepareMessage) (*DataFlowResponseMessage, error) {
	return dsdk.processFlow(ctx, message.ProcessId, Completed, func(ctx context.Context, flow *DataFlow) (*DataFlowResponseMessage, error) {
		response, err := dsdk.OnPrepare(ctx, flow)
		if err != nil {
			return nil, fmt.Errorf("provision data flow: %w", err)
		}
		return response, nil
	})
}

// Start is called on the provider and starts a data flow based on the given start message and execution context.
// It invokes the OnStart callback and persists the created flow. Returns a response or an error if the process fails.
func (dsdk *DataPlaneSDK) Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error) {
	return dsdk.processFlow(ctx, message.ProcessId, Started, func(ctx context.Context, flow *DataFlow) (*DataFlowResponseMessage, error) {
		response, err := dsdk.OnStart(ctx, flow)
		if err != nil {
			return nil, fmt.Errorf("start data flow: %w", err)
		}
		return response, nil
	})
}

func (dsdk *DataPlaneSDK) Terminate(ctx context.Context, processId string) error {
	if processId == "" {
		return errors.New("processId cannot be empty")
	}

	return dsdk.execute(ctx, func(ctx context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil {
			return fmt.Errorf("performing terminate de-duplication: %w", err)
		}

		if Terminated == flow.State {
			return nil // duplicate message, skip processing
		}

		return dsdk.updateFlowState(ctx, processId, Terminated, func(flow *DataFlow) error {
			if err := dsdk.OnTerminate(ctx, flow); err != nil {
				return fmt.Errorf("terminate data flow: %w", err)
			}
			return nil
		})
	})
}

func (dsdk *DataPlaneSDK) Suspend(ctx context.Context, processId string) error {
	if processId == "" {
		return errors.New("processId cannot be empty")
	}

	return dsdk.execute(ctx, func(ctx2 context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil {
			return fmt.Errorf("performing suspend de-duplication: %w", err)
		}

		if Suspended == flow.State {
			return nil // duplicate message, skip processing
		}

		return dsdk.updateFlowState(ctx, processId, Suspended, func(flow *DataFlow) error {
			if err := dsdk.OnSuspend(ctx, flow); err != nil {
				return fmt.Errorf("suspend data flow: %w", err)
			}
			return nil
		})
	})
}

func (dsdk *DataPlaneSDK) Recover(ctx context.Context) error {
	return dsdk.execute(ctx, func(ctx2 context.Context) error {
		iter := dsdk.Store.AcquireDataFlowsForRecovery(ctx)
		if iter == nil {
			return errors.New("failed to create iterator")
		}
		//nolint:errcheck
		defer iter.Close()

		var errs []error
		for iter.Next() {
			flow := iter.Get()
			if flow == nil {
				continue // skip nil flows
			}
			if err := dsdk.OnRecover(ctx, flow); err != nil {
				errs = append(errs, fmt.Errorf("data flow %v: %w", flow.ID, err))
			}
		}

		if err := iter.Error(); err != nil {
			return fmt.Errorf("recovering data flows: %w", err)
		}

		return errors.Join(errs...)
	})
}

// processFlow handles common flow operations with deduplication
func (dsdk *DataPlaneSDK) processFlow(
	ctx context.Context,
	processId string,
	expectedState DataFlowState,
	processor DataFlowProcessor,
) (*DataFlowResponseMessage, error) {
	if processId == "" {
		return nil, errors.New("processId cannot be empty")
	}

	var response *DataFlowResponseMessage
	err := dsdk.execute(ctx, func(ctx2 context.Context) error {
		flow, err := dsdk.Store.FindById(ctx, processId)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("performing de-duplication: %w", err)
		}

		switch {
		case flow != nil && flow.State == expectedState:
			// duplicate message, skip processing
			response = &DataFlowResponseMessage{}
			return nil
		case flow != nil && flow.State != expectedState:
			return fmt.Errorf("data flow exists and %v is not in %v state", flow.ID, expectedState)
		}

		response, err = processor(ctx, flow)
		if err != nil {
			return err
		}

		if err := dsdk.Store.Create(ctx, flow); err != nil {
			return fmt.Errorf("creating data flow: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (dsdk *DataPlaneSDK) execute(ctx context.Context, callback func(ctx2 context.Context) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return dsdk.TrxContext.Execute(ctx, callback)
	}
}

func (dsdk *DataPlaneSDK) updateFlowState(ctx context.Context, id string, newState DataFlowState, callback func(*DataFlow) error) error {
	flow, err := dsdk.Store.FindById(ctx, id)
	if err != nil {
		return fmt.Errorf("finding data flow for id %v: %w", id, err)
	}

	if err := callback(flow); err != nil {
		return err
	}

	flow.State = newState
	if err := dsdk.Store.Save(ctx, flow); err != nil {
		return fmt.Errorf("saving data flow: %w", err)
	}
	return nil
}

type DataPlaneSDKBuilder struct {
	sdk *DataPlaneSDK
}

func NewDataPlaneSDKBuilder() *DataPlaneSDKBuilder {
	return &DataPlaneSDKBuilder{
		sdk: &DataPlaneSDK{},
	}
}

func (b *DataPlaneSDKBuilder) Store(store DataplaneStore) *DataPlaneSDKBuilder {
	b.sdk.Store = store
	return b
}

func (b *DataPlaneSDKBuilder) TransactionContext(trxContext TransactionContext) *DataPlaneSDKBuilder {
	b.sdk.TrxContext = trxContext
	return b
}

func (b *DataPlaneSDKBuilder) OnPrepare(handler DataFlowProcessor) *DataPlaneSDKBuilder {
	b.sdk.OnPrepare = handler
	return b
}

func (b *DataPlaneSDKBuilder) OnStart(handler DataFlowProcessor) *DataPlaneSDKBuilder {
	b.sdk.OnStart = handler
	return b
}

func (b *DataPlaneSDKBuilder) OnTerminate(handler DataFlowHandler) *DataPlaneSDKBuilder {
	b.sdk.OnTerminate = handler
	return b
}

func (b *DataPlaneSDKBuilder) OnSuspend(handler DataFlowHandler) *DataPlaneSDKBuilder {
	b.sdk.OnSuspend = handler
	return b
}

func (b *DataPlaneSDKBuilder) OnRecover(handler DataFlowHandler) *DataPlaneSDKBuilder {
	b.sdk.OnRecover = handler
	return b
}

func (b *DataPlaneSDKBuilder) Build() (*DataPlaneSDK, error) {
	if b.sdk.Store == nil {
		return nil, errors.New("store is required")
	}
	if b.sdk.TrxContext == nil {
		return nil, errors.New("transaction context is required")
	}
	if b.sdk.OnPrepare == nil {
		return nil, errors.New("OnPrepare handler is required")
	}
	if b.sdk.OnStart == nil {
		return nil, errors.New("OnStart handler is required")
	}
	if b.sdk.OnTerminate == nil {
		return nil, errors.New("OnTerminate handler is required")
	}
	if b.sdk.OnSuspend == nil {
		return nil, errors.New("OnSuspend handler is required")
	}
	if b.sdk.OnRecover == nil {
		return nil, errors.New("OnRecover handler is required")
	}

	return b.sdk, nil
}
