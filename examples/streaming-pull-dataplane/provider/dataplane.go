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
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/streaming-pull-dataplane/config"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
	"log"
	"net/http"
)

// ProviderDataPlane demonstrates how to use the Data Plane SDK. This implementation supports pull event streaming.
type ProviderDataPlane struct {
	api              *dsdk.DataPlaneApi
	signallingServer *http.Server
	authService      *AuthService
	natsServer       *NATSServer
	publisherService *EventPublisherService
}

func NewDataPlane(authService *AuthService, natServer *NATSServer, publisherApp *EventPublisherService) (*ProviderDataPlane, error) {
	providerDataPlane := &ProviderDataPlane{authService: authService, natsServer: natServer, publisherService: publisherApp}

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

	// Start signaling server
	go func() {
		log.Printf("[Provider Data Plane] Signalling server listening on port %d\n", common.ProviderSignallingPort)
		if err := d.signallingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Provider signaling server failed to start: %v", err)
		}
	}()
}

func (d *ProviderDataPlane) Shutdown(ctx context.Context) {
	if d.signallingServer != nil {
		if err := d.signallingServer.Shutdown(ctx); err != nil {
			log.Printf("Provider signalling server shutdown error: %v", err)
		}
	}
	log.Println("Provider data plane shutdown")
}

func (d *ProviderDataPlane) prepareProcessor(_ context.Context,
	_ *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	_ *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	return nil, errors.New("not supported on provider")
}

func (d *ProviderDataPlane) startProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	if options.Duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
	}

	err := flow.TransitionToStarting()
	if err != nil {
		return nil, err
	}
	token, err := d.authService.CreateToken(flow.ID)
	if err != nil {
		return nil, err
	}

	channel := flow.ID + "." + config.ForwardSuffix
	replyChannel := flow.ID + "." + config.ReplySuffix
	da, err := dsdk.NewDataAddressBuilder().
		Property(dsdk.EndpointType, config.NATSEndpointType).
		Property(dsdk.EndpointKey, natsUrl).
		EndpointProperty(config.TokenKey, "string", token).
		EndpointProperty(config.ChannelKey, "string", channel).
		EndpointProperty(config.ReplyChannelKey, "string", replyChannel).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}
	err = flow.TransitionToStarted()
	if err != nil {
		return nil, err
	}

	// Start publishing events. In a real system, this could be done via a queue or notification mechanism
	d.publisherService.Start(channel)

	log.Printf("[Provider Data Plane] Starting transfer for %s\n", flow.CounterPartyId)
	return &dsdk.DataFlowResponseMessage{State: flow.State, DataAddress: *da}, nil
}

func (d *ProviderDataPlane) suspendProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	err := flow.TransitionToSuspended()
	channel := flow.ID + "." + config.ForwardSuffix
	d.publisherService.Terminate(channel)
	if err != nil {
		return err
	}
	log.Printf("[Provider Data Plane] Suspending transfer for %s\n", flow.CounterPartyId)
	return d.invalidate(flow)
}

func (d *ProviderDataPlane) terminateProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	err := flow.TransitionToTerminated()
	channel := flow.ID + "." + config.ForwardSuffix
	d.publisherService.Terminate(channel)
	if err != nil {
		return err
	}
	log.Printf("[Provider Data Plane] Terminating transfer for %s\n", flow.CounterPartyId)
	return d.invalidate(flow)
}

func (d *ProviderDataPlane) invalidate(flow *dsdk.DataFlow) error {
	// Connection terminated after to avoid case where a client reconnects before the token is invalidated
	defer d.natsServer.InvalidateConnection(flow.ID)
	err := d.authService.InvalidateToken(flow.ID)
	if err != nil {
		return err
	}
	return nil
}

func (d *ProviderDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}
