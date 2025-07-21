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
	"time"

	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
)

const startUrl = "http://localhost:%d/start"

func DataFlowStart(ctx context.Context) error {
	startMessage := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageId:     uuid.NewString(),
			ProcessId:     uuid.NewString(),
			ParticipantId: "did:web:example.com",
			AgreementId:   uuid.NewString(),
		},
	}

	serialized, err := json.Marshal(startMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal start message: %w", err)
	}

	// Create the request
	url := fmt.Sprintf(startUrl, common.SignallingPort)
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

	fmt.Printf("DataFlowStartMessage successful: %v\n", message)
	return nil
}
