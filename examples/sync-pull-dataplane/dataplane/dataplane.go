//  Copyright (c) 2025 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
//       Metaform Systems, Inc. - initial API and implementation
//

package dataplane

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
	"log"
	"net/http"
)

func NewSdkApi() *dsdk.DataPlaneApi {
	dataplane := newDataPlane()

	builder := dsdk.NewDataPlaneSDKBuilder()
	store := memory.NewInMemoryStore()
	builder.Store(store)
	builder.TransactionContext(memory.InMemoryTrxContext{})
	builder.OnPrepare(dataplane.prepareProcessor)
	builder.OnStart(dataplane.startProcessor)
	builder.OnRecover(dataplane.noopHandler)
	builder.OnSuspend(dataplane.suspendProcessor)
	builder.OnTerminate(dataplane.terminateProcessor)
	builder.OnRecover(dataplane.noopHandler)

	sdk, err := builder.Build()
	if err != nil {
		panic(err)
	}

	return dsdk.NewDataPlaneApi(sdk)
}

type ProviderDataPlane struct {
	TokenStore *common.Store[string]
}

func newDataPlane() *ProviderDataPlane {
	return &ProviderDataPlane{
		TokenStore: common.NewStore[string](),
	}
}

func (d *ProviderDataPlane) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, duplicate bool, sdk *dsdk.DataPlaneSDK) (*dsdk.DataFlowResponseMessage, error) {
	return nil, errors.New("not supported on provider")
}

func (d *ProviderDataPlane) startProcessor(ctx context.Context, flow *dsdk.DataFlow, duplicate bool, sdk *dsdk.DataPlaneSDK) (*dsdk.DataFlowResponseMessage, error) {
	// Generate token once for both paths
	token := uuid.NewString()

	if duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
		d.TokenStore.Delete(flow.ID)
	}

	// Store token first, then build data address
	d.TokenStore.Create(flow.ID, token) // token is pinned to the flow ID which is the transfer process id on the control plane
	err := flow.TransitionToStarted()
	if err != nil {
		return nil, err
	}

	da, err := dsdk.NewDataAddressBuilder().Property("token", token).Build()
	if err != nil {
		// remove up token on error
		d.TokenStore.Delete(flow.ID)
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}

	log.Println("Started transfer")
	return &dsdk.DataFlowResponseMessage{State: flow.State, DataAddress: *da}, nil
}

func (d *ProviderDataPlane) suspendProcessor(ctx context.Context, flow *dsdk.DataFlow) error {
	d.TokenStore.Delete(flow.ID) // invalidate token
	err := flow.TransitionToSuspended()
	if err != nil {
		return err
	}
	return nil
}

func (d *ProviderDataPlane) terminateProcessor(ctx context.Context, flow *dsdk.DataFlow) error {
	// TODO add reason code
	d.TokenStore.Delete(flow.ID) // invalidate token
	err := flow.TransitionToTerminated()
	if err != nil {
		return err
	}
	return nil
}

func (d *ProviderDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}

func NewSignallingServer(sdkApi *dsdk.DataPlaneApi) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", sdkApi.Start)
	mux.HandleFunc("/prepare", sdkApi.Prepare)

	return &http.Server{Addr: fmt.Sprintf(":%d", common.SignallingPort), Handler: mux}
}

func NewDataServer() *http.Server {
	mux := http.NewServeMux()
	return &http.Server{Addr: fmt.Sprintf(":%d", common.DataPort), Handler: mux}
}
