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

package app

import (
	"context"
	"errors"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
	"log"
	"net/http"
)

type ClientApp struct {
	api              *dsdk.DataPlaneApi
	signallingServer *http.Server
}

func NewClientApp() (*ClientApp, error) {
	app := &ClientApp{}
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

	return &ClientApp{api: api}, nil
}

func (a *ClientApp) Init() {
	a.signallingServer = common.NewSignallingServer(a.api, common.ConsumerSignallingPort)
	// Start signaling server
	go func() {
		log.Printf("Consumer signalling server listening on port %d\n", common.ConsumerSignallingPort)
		if err := a.signallingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Signaling server failed to start: %v", err)
		}
	}()
}

func (a *ClientApp) Shutdown(ctx context.Context) {
	if a.signallingServer != nil {
		if err := a.signallingServer.Shutdown(ctx); err != nil {
			log.Printf("App signalling server shutdown error: %v", err)
		}
	}
	log.Println("Consumer app shutdown")
}

func (a *ClientApp) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	return &dsdk.DataFlowResponseMessage{State: dsdk.Prepared}, nil
}

func (a *ClientApp) startProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	return &dsdk.DataFlowResponseMessage{State: dsdk.Started}, nil
}

func (a *ClientApp) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}
