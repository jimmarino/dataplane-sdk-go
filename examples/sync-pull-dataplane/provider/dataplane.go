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

package provider

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

type ProviderDataPlane struct {
	api              *dsdk.DataPlaneApi
	tokenStore       *common.Store[string]
	signallingServer *http.Server
	dataServer       *http.Server
}

func NewDataPlane() (*ProviderDataPlane, error) {
	providerDataPlane := &ProviderDataPlane{
		tokenStore: common.NewStore[string](),
	}

	builder := dsdk.NewDataPlaneSDKBuilder()
	store := memory.NewInMemoryStore()
	sdk, err := builder.Store(store).
		TransactionContext(memory.InMemoryTrxContext{}).
		OnPrepare(providerDataPlane.prepareProcessor).
		OnStart(providerDataPlane.startProcessor).
		OnRecover(providerDataPlane.noopHandler).
		OnSuspend(providerDataPlane.suspendProcessor).
		OnTerminate(providerDataPlane.terminateProcessor).
		OnRecover(providerDataPlane.noopHandler).
		Build()
	if err != nil {
		return nil, err
	}

	providerDataPlane.api = dsdk.NewDataPlaneApi(sdk)

	return providerDataPlane, nil
}

func (d *ProviderDataPlane) Init() {

	d.signallingServer = common.NewSignallingServer(d.api, common.ProviderSignallingPort)
	d.dataServer = common.NewDataServer(common.DataPort)

	// Start signaling server
	go func() {
		log.Printf("[Provider Data Plane] Signalling server listening on port %d\n", common.ProviderSignallingPort)
		if err := d.signallingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Provider signaling server failed to start: %v", err)
		}
	}()

	// Start data server
	go func() {
		log.Printf("[Provider Data Plane] Data server listening on port %d\n", common.DataPort)
		if err := d.dataServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Provider data server failed to start: %v", err)
		}
	}()

}

func (d *ProviderDataPlane) Shutdown(ctx context.Context) {
	if d.signallingServer != nil {
		if err := d.signallingServer.Shutdown(ctx); err != nil {
			log.Printf("Provider signalling server shutdown error: %v", err)
		}
	}
	if d.dataServer != nil {
		if err := d.dataServer.Shutdown(ctx); err != nil {
			log.Printf("Provider data server shutdown error: %v", err)
		}
	}
	log.Println("Provider data plane shutdown")
}

func (d *ProviderDataPlane) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	return nil, errors.New("not supported on provider")
}

func (d *ProviderDataPlane) startProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	// Generate token once for both paths
	token := uuid.NewString()

	if options.Duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
		d.tokenStore.Delete(flow.ID)
	}

	// Store token first, then build data address
	d.tokenStore.Create(flow.ID, token) // token is pinned to the flow ID which is the transfer process id on the control plane
	err := flow.TransitionToStarted()
	if err != nil {
		return nil, err
	}

	da, err := dsdk.NewDataAddressBuilder().Property("token", token).Build()
	if err != nil {
		// remove up token on error
		d.tokenStore.Delete(flow.ID)
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}

	log.Printf("[Provider Data Plane] Started transfer for %s and returning access token\n", flow.CounterPartyId)
	return &dsdk.DataFlowResponseMessage{State: flow.State, DataAddress: *da}, nil
}

func (d *ProviderDataPlane) suspendProcessor(ctx context.Context, flow *dsdk.DataFlow) error {
	d.tokenStore.Delete(flow.ID) // invalidate token
	err := flow.TransitionToSuspended()
	if err != nil {
		return err
	}
	return nil
}

func (d *ProviderDataPlane) terminateProcessor(ctx context.Context, flow *dsdk.DataFlow) error {
	// TODO add reason code
	d.tokenStore.Delete(flow.ID) // invalidate token
	err := flow.TransitionToTerminated()
	if err != nil {
		return err
	}
	return nil
}

func (d *ProviderDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}
