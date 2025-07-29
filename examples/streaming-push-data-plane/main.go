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
	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/controlplane"
	"github.com/metaform/dataplane-sdk-go/examples/streaming-push-data-plane/launcher"
	"log"
	"time"
)

func main() {
	launcher.LaunchServices()

	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v\n", err)
	}

	ctx := context.Background()
	defer ctx.Done()

	agreementID := uuid.NewString()
	datasetID := uuid.NewString()

	consumerProcessID := uuid.NewString()
	da, err := cp.ConsumerPrepare(ctx, consumerProcessID, agreementID, datasetID)
	if err != nil {
		log.Fatalf("Unable to send prepare to consumer control plane: %v\n", err)
	}
	providerProcessID := uuid.NewString()

	// Signal to the provider data plane
	_, err = cp.ProviderStart(ctx, providerProcessID, agreementID, datasetID, da)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	err = cp.ConsumerStart(ctx, consumerProcessID, da)
	if err != nil {
		log.Fatalf("Unable to send start to consumer control plane: %v\n", err)
	}

	// Simulate ongoing processing
	time.Sleep(10 * time.Second)

}
