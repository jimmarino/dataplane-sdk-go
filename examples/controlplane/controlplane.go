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

package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
)

const startUrl = "http://localhost:%d/start"
const consumerPrepareUrl = "http://localhost:%d/prepare"

// ControlPlaneSimulator simulates control plane interactions between a consumer and provider and drives their respective data planes.
type ControlPlaneSimulator struct {
	consumerDataPlane string
	providerDataPlane string
}

func NewSimulator() (*ControlPlaneSimulator, error) {
	return &ControlPlaneSimulator{}, nil
}

func (c *ControlPlaneSimulator) ProviderStart(ctx context.Context, processId string, agreementId string, datasetId string) (*dsdk.DataAddress, error) {

	callbackURL, _ := url.Parse("http://provider.com/dp/callback")

	startMessage := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageId:        uuid.NewString(),
			AgreementId:      agreementId,
			DatasetId:        datasetId,
			ProcessId:        processId,
			DataspaceContext: "dscontext",
			CounterPartyId:   "did:web:consumer.com",
			ParticipantId:    "did:web:provider.com",
			CallbackAddress:  dsdk.CallbackURL(*callbackURL),
			TransferType:     dsdk.TransferType{DestinationType: "custom", FlowType: dsdk.Pull},
		},
	}

	serialized, err := json.Marshal(startMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal start message: %w", err)
	}

	// Create the request
	url := fmt.Sprintf(startUrl, common.ProviderSignallingPort)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(serialized))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("start request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var message dsdk.DataFlowResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &message.DataAddress, nil
}

func (c *ControlPlaneSimulator) ConsumerStart(ctx context.Context, processId string, source dsdk.DataAddress) error {
	callbackURL, _ := url.Parse("http://provider.com/dp/callback")
	startMessage := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageId:        uuid.NewString(),
			ProcessId:        processId,
			AgreementId:      uuid.NewString(),
			DataspaceContext: "dscontext",
			ParticipantId:    "did:web:consumer.com",
			CounterPartyId:   "did:web:provider.com",
			CallbackAddress:  dsdk.CallbackURL(*callbackURL),
			TransferType:     dsdk.TransferType{DestinationType: "custom", FlowType: dsdk.Pull},
		},
		SourceDataAddress: source,
	}

	serialized, err := json.Marshal(startMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal start message: %w", err)
	}

	// Create the request
	url := fmt.Sprintf(startUrl, common.ConsumerSignallingPort)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("start request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var message dsdk.DataFlowResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

func (c *ControlPlaneSimulator) ConsumerPrepare(ctx context.Context, processId string, agreementId string, datasetId string) error {
	callbackURL, _ := url.Parse("http://provider.com/dp/callback")
	prepareMessage := dsdk.DataFlowPrepareMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageId:        uuid.NewString(),
			AgreementId:      agreementId,
			DatasetId:        datasetId,
			ProcessId:        processId,
			DataspaceContext: "dscontext",
			ParticipantId:    "did:web:consumer.com",
			CounterPartyId:   "did:web:provider.com",
			CallbackAddress:  dsdk.CallbackURL(*callbackURL),
			TransferType:     dsdk.TransferType{DestinationType: "custom", FlowType: dsdk.Pull},
		},
	}

	serialized, err := json.Marshal(prepareMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal prepare message: %w", err)
	}

	// Create the request
	url := fmt.Sprintf(consumerPrepareUrl, common.ConsumerSignallingPort)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("prepare request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var message dsdk.DataFlowResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}
