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

package consumer

import (
	"context"
	"errors"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
	"log"
	"net/http"
)

type ConsumerDataPlane struct {
	api              *dsdk.DataPlaneApi
	signallingServer *http.Server
}

func NewDataPlane() (*ConsumerDataPlane, error) {
	app := &ConsumerDataPlane{}
	sdk, err := dsdk.NewDataPlaneSDKBuilder().
		Store(memory.NewInMemoryStore()).
		TransactionContext(memory.InMemoryTrxContext{}).
		OnPrepare(app.prepareProcessor).
		OnStart(app.startProcessor).
		OnTerminate(app.noopHandler).
		OnSuspend(app.noopHandler).
		OnRecover(app.noopHandler).
		Build()
	if err != nil {
		return nil, err
	}
	api := dsdk.NewDataPlaneApi(sdk)

	return &ConsumerDataPlane{api: api}, nil
}

func (a *ConsumerDataPlane) Init() {
	a.signallingServer = common.NewSignallingServer(a.api, common.ConsumerSignallingPort)
	// Start signaling server
	go func() {
		log.Printf("[Consumer Data Plane] Signalling server listening on port %d\n", common.ConsumerSignallingPort)
		if err := a.signallingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Consumer signaling server failed to start: %v", err)
		}
	}()
}

func (a *ConsumerDataPlane) Shutdown(ctx context.Context) {
	if a.signallingServer != nil {
		if err := a.signallingServer.Shutdown(ctx); err != nil {
			log.Printf("Consumer signalling server shutdown error: %v", err)
		}
	}
	log.Println("Consumer data plane shutdown")
}

func (a *ConsumerDataPlane) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	log.Printf("[Consumer Data Plane] Prepared transfer for participant %s dataset %s\n", flow.ParticipantId, flow.DatasetId)
	return &dsdk.DataFlowResponseMessage{State: dsdk.Prepared}, nil
}

func (a *ConsumerDataPlane) startProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	log.Printf("[Consumer Data Plane] Transfer access token available for participant %s dataset %s\n", flow.ParticipantId, flow.DatasetId)
	return &dsdk.DataFlowResponseMessage{State: dsdk.Started}, nil
}

func (a *ConsumerDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}
