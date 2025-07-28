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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/controlplane"
	"github.com/metaform/dataplane-sdk-go/examples/sync-pull-dataplane/launcher"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	jsonContentType = "application/json"
	accept          = "Accept"
	authorization   = "Authorization"
)

// Demonstrates initiating a data transfer using a provider data plane that implements synchronous signalling start operations.
func main() {

	var wg sync.WaitGroup
	defer wg.Done()
	wg.Add(1)

	launcher.LaunchServicesAndWait(&wg)

	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v\n", err)
	}

	ctx := context.Background()
	defer ctx.Done()

	agreementId := uuid.NewString()
	datasetId := uuid.NewString()

	consumerProcessId := uuid.NewString()
	err = cp.ConsumerPrepare(ctx, consumerProcessId, agreementId, datasetId)
	if err != nil {
		log.Fatalf("Unable to send prepate to consumer control plane: %v\n", err)
	}

	// Signal to the provider data plane
	providerProcessId := uuid.NewString()
	da, err := cp.ProviderStart(ctx, providerProcessId, agreementId, datasetId)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	// Signal to the consumer data plane in response
	err = cp.ConsumerStart(ctx, consumerProcessId, *da)
	if err != nil {
		log.Fatalf("Unable to send start to consumer control plane: %v\n", err)
	}

	if err = transferDataset(datasetId); err != nil {
		log.Fatalf("Unable to start data transfer: %v\n", err)
	}
}

func transferDataset(datasetId string) error {
	if datasetId == "" {
		return fmt.Errorf("dataset ID cannot be empty")
	}

	ctx := context.Background()
	defer ctx.Done()
	consumerEndpoint := fmt.Sprintf("http://localhost:%d/tokens/%s", common.ConsumerDataPort, datasetId)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, consumerEndpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create token request: %w", err)
	}

	httpClient := http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResponse common.TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResponse); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s", tokenResponse.Endpoint, datasetId), nil)
	if err != nil {
		return fmt.Errorf("failed to create dataset request: %w", err)
	}
	req.Header.Set(authorization, fmt.Sprintf("Bearer %s", tokenResponse.Token))
	req.Header.Set(accept, jsonContentType)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response: %d", resp.StatusCode)

	}
	log.Printf("[Client] Successful dataset request")
	return nil
}
