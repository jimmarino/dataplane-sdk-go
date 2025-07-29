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

package provider

import (
	"context"
	"fmt"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

// EventPublisherService mocks a service that publishes event streams intended for clients. Event streams are managed by
// the provider data plane.
type EventPublisherService struct {
	publisherStore *common.Store[*storeEntry]
}

func NewEventPublisherService() *EventPublisherService {
	return &EventPublisherService{publisherStore: common.NewStore[*storeEntry]()}
}

func (m *EventPublisherService) Start(id string, endpoint string, channel string, token string) {
	ctx, cancellation := context.WithCancel(context.Background())
	m.publisherStore.Create(id, &storeEntry{id: id, channel: channel, endpoint: endpoint, token: token, cancellation: &cancellation})
	go m.startInternal(ctx, channel)
}

func (m *EventPublisherService) Terminate(id string) {
	cancellation, found := m.publisherStore.Find(id)
	if !found {
		return
	}
	m.publisherStore.Delete(id)
	(*cancellation.cancellation)()
}

func (m *EventPublisherService) startInternal(ctx context.Context, id string) {
	defer ctx.Done()
	entry, found := m.publisherStore.Find(id)
	if !found {
		log.Printf("[Event Publisher] Failed to find publisher entry for ID %s", id)
		return
	}
	nc, err := connect(entry)
	if err != nil {
		log.Printf("[Event Publisher] Failed to connect to NATS: %v", err)
		return
	}
	defer nc.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Event Publisher] Event publishing cancelled: %v", ctx.Err())
			return
		case <-ticker.C:
			log.Printf("Sending event: %d\n", i)
			err := nc.Publish(entry.channel, []byte(fmt.Sprintf(`{"data": "Event %d"}`, i)))
			if err != nil {
				log.Printf("[Event Publisher] Failed to publish event: %v", err)
				return
			}
			i++
		}
	}
}

func connect(entry *storeEntry) (*nats.Conn, error) {
	nc, err := nats.Connect(entry.endpoint,
		nats.Token(entry.token),
	)
	if err != nil {
		return nil, err
	}
	log.Println("Provider client connected to provider NATS")
	return nc, nil
}

type storeEntry struct {
	id           string
	channel      string
	endpoint     string
	token        string
	cancellation *context.CancelFunc
}
