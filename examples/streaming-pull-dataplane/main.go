package main

import (
	"context"
	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/controlplane"
	"github.com/metaform/dataplane-sdk-go/examples/streaming-pull-dataplane/launcher"
	"log"
	"time"
)

const (
	natsUrl = "localhost:4222"
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
	err = cp.ConsumerPrepare(ctx, consumerProcessID, agreementID, datasetID)
	if err != nil {
		log.Fatalf("Unable to send prepare to consumer control plane: %v\n", err)
	}

	providerProcessID := uuid.NewString()

	// Signal to the provider data plane
	da, err := cp.ProviderStart(ctx, providerProcessID, agreementID, datasetID)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	err = cp.ConsumerStart(ctx, consumerProcessID, *da)
	if err != nil {
		log.Fatalf("Unable to send start to consumer control plane: %v\n", err)
	}

	// Simulate ongoing processing
	time.Sleep(10 * time.Second)

	// Terminate the transfer and forcibly disconnect the client
	err = cp.ProviderTerminate(ctx, providerProcessID, agreementID, datasetID)
	if err != nil {
		log.Fatalf("Unable to send terminate to provider control plane: %v\n", err)
	}

	// Simulate ongoing processing until terminate is completed
	time.Sleep(2 * time.Second)

}
