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
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/sync-pull-dataplane/dataplane"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Creates an HTTP data plane that implements synchronous prepare and start messages.
func main() {
	sdkApi := dataplane.NewSdkApi()

	signallingServer := dataplane.NewSignallingServer(sdkApi)
	dataServer := dataplane.NewDataServer()

	// Start signaling server
	go func() {
		log.Printf("Signalling server listening on port %d\n", common.SignallingPort)
		if err := signallingServer.ListenAndServe(); err != nil {
			log.Fatalf("Signaling server failed to start: %v", err)
		}
	}()

	// Start data server
	go func() {
		log.Printf("Data server listening on port %d\n", common.DataPort)
		if err := dataServer.ListenAndServe(); err != nil {
			log.Fatalf("Signaling server failed to start: %v", err)
		}
	}()

	// Wait for SIGINT
	channel := make(chan os.Signal, 1)
	signal.Notify(channel, syscall.SIGINT)
	<-channel

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := signallingServer.Shutdown(ctx); err != nil {
		log.Printf("Signalling server shutdown error: %v", err)
	}
	if err := dataServer.Shutdown(ctx); err != nil {
		log.Printf("Data server shutdown error: %v", err)
	}
	log.Println("Data plane stopped")

}
