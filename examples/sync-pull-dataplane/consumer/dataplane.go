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
	"encoding/json"
	"errors"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
	"log"
	"net/http"
)

const (
	contentType     = "Content-Type"
	jsonContentType = "application/json"
)

// ConsumerDataPlane is a consumer data plane that demonstrates how to use the Data Plane SDK. This implementation supports
// the transfer of simple JSON datasets over HTTP and Data Plane Signalling start and prepare handling using synchronous responses.
// After a transfer is started, clients obtain the access token from this data plane and issue the request to the provider data plane.
//
// Note that this data plane does not proxy requests to the provider data plane. This is recommended best practice as it avoids
// unnecessary overhead and a potential failure point.
//
// This data plane implements non-finite data transfers. Multiple requests may be issued to the provider data plane over a
// period of time. For example, the dataset could be access to an API.
type ConsumerDataPlane struct {
	api              *dsdk.DataPlaneApi
	signallingServer *http.Server
	dataServer       *http.Server
	tokenStore       *common.Store[tokenEntry]
}

func NewDataPlane() (*ConsumerDataPlane, error) {
	dataplane := &ConsumerDataPlane{tokenStore: common.NewStore[tokenEntry]()}
	sdk, err := dsdk.NewDataPlaneSDKBuilder().
		Store(memory.NewInMemoryStore()).
		TransactionContext(memory.InMemoryTrxContext{}).
		OnPrepare(dataplane.prepareProcessor).
		OnStart(dataplane.startProcessor).
		OnTerminate(dataplane.noopHandler).
		OnSuspend(dataplane.noopHandler).
		OnRecover(dataplane.noopHandler).
		Build()
	if err != nil {
		return nil, err
	}
	dataplane.api = dsdk.NewDataPlaneApi(sdk)
	return dataplane, nil
}

func (d *ConsumerDataPlane) Init() {
	d.signallingServer = common.NewSignallingServer(d.api, common.ConsumerSignallingPort)
	// Start signaling server
	go func() {
		log.Printf("[Consumer Data Plane] Signalling server listening on port %d\n", common.ConsumerSignallingPort)
		if err := d.signallingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Consumer signaling server failed to start: %v", err)
		}
	}()

	d.dataServer = common.NewDataServer(common.ConsumerDataPort, "/tokens/", d.getEndpointToken)
	go func() {
		log.Printf("[Consumer Data Plane] Data server listening on port %d\n", common.ConsumerDataPort)
		if err := d.dataServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Provider data server failed to start: %v", err)
		}
	}()
}

func (d *ConsumerDataPlane) Shutdown(ctx context.Context) {
	if d.signallingServer != nil {
		if err := d.signallingServer.Shutdown(ctx); err != nil {
			log.Printf("Consumer signalling server shutdown error: %v", err)
		}
	}
	log.Println("Consumer data plane shutdown")
}

func (d *ConsumerDataPlane) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	log.Printf("[Consumer Data Plane] Prepared transfer for participant %s dataset %s\n", flow.ParticipantId, flow.DatasetId)
	return &dsdk.DataFlowResponseMessage{State: dsdk.Prepared}, nil
}

func (d *ConsumerDataPlane) startProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	log.Printf("[Consumer Data Plane] Transfer access token available for participant %s dataset %s\n", flow.ParticipantId, flow.DatasetId)
	endpoint := options.SourceDataAddress.Properties[dsdk.EndpointKey].(string)
	token := options.SourceDataAddress.Properties["token"].(string)
	d.tokenStore.Create(flow.DatasetId, tokenEntry{datasetId: flow.DatasetId, token: token, endpoint: endpoint})
	return &dsdk.DataFlowResponseMessage{State: dsdk.Started}, nil
}

func (d *ConsumerDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}

func (d *ConsumerDataPlane) getEndpointToken(w http.ResponseWriter, r *http.Request) {
	// Check if it's a GET request
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	datasetId, err := common.ParseDataset(w, r)
	if err != nil {
		http.Error(w, "Invalid URL path: "+err.Error(), http.StatusBadRequest)
		return
	}
	entry, exists := d.tokenStore.Find(datasetId)
	if !exists {
		http.Error(w, "Token not found for dataset", http.StatusNotFound)
		return
	}

	response := common.TokenResponse{
		Token:    entry.token,
		Endpoint: entry.endpoint,
	}

	w.Header().Set(contentType, jsonContentType)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

type tokenEntry struct {
	datasetId string
	token     string
	endpoint  string
}
