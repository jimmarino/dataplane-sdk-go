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
	"fmt"
	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/controlplane"
	"github.com/metaform/dataplane-sdk-go/examples/sync-pull-dataplane/launcher"
	"sync"
)

// Demonstrates initiating a data transfer using a provider data plane that implements synchronous signalling start operations.
func main() {

	var wg sync.WaitGroup
	defer wg.Done()
	wg.Add(1)

	launcher.LaunchServicesAndWait(&wg)

	cp, err := controlplane.NewSimulator()

	if err != nil {
		fmt.Printf("Unable to launch control plane simulator: %v\n", err)
	}
	ctx := context.Background()
	defer ctx.Done()

	agreementId := uuid.NewString()
	datasetId := uuid.NewString()

	// Signal to the provider data plane
	providerProcessId := uuid.NewString()
	da, err := cp.ProviderStart(ctx, providerProcessId, agreementId, datasetId)
	if err != nil {
		panic(err)
	}

	consumerProcessId := uuid.NewString()
	err = cp.ConsumerPrepare(ctx, consumerProcessId, agreementId, datasetId)
	if err != nil {
		panic(err)
	}

	// Signal to the consumer data plane in response
	err = cp.ConsumerStart(ctx, consumerProcessId, *da)
	if err != nil {
		panic(err)
	}

}
