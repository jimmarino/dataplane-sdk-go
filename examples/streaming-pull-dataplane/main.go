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

	agreementId := uuid.NewString()
	datasetId := uuid.NewString()

	consumerProcessId := uuid.NewString()
	err = cp.ConsumerPrepare(ctx, consumerProcessId, agreementId, datasetId)
	if err != nil {
		log.Fatalf("Unable to send prepare to consumer control plane: %v\n", err)
	}

	providerProcessId := uuid.NewString()

	// Signal to the provider data plane
	da, err := cp.ProviderStart(ctx, providerProcessId, agreementId, datasetId)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	err = cp.ConsumerStart(ctx, consumerProcessId, *da)
	if err != nil {
		log.Fatalf("Unable to send start to consumer control plane: %v\n", err)
	}

	// Simulate ongoing processing
	time.Sleep(10 * time.Second)

	// Terminate the transfer and forcibly disconnect the client
	err = cp.ProviderTerminate(ctx, providerProcessId, agreementId, datasetId)
	if err != nil {
		log.Fatalf("Unable to send terminate to provider control plane: %v\n", err)
	}

	// Simulate ongoing processing until terminate is completed
	time.Sleep(2 * time.Second)
	
}
