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

package common

import (
	"fmt"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"net/http"
)

const ProviderSignallingPort = 8080
const DataPort = 8181

const AppPort = 9090
const ConsumerSignallingPort = 9191

func NewSignallingServer(sdkApi *dsdk.DataPlaneApi, port int) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", sdkApi.Start)
	mux.HandleFunc("/prepare", sdkApi.Prepare)

	return &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
}

func NewDataServer(port int) *http.Server {
	mux := http.NewServeMux()
	return &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
}
