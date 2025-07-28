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

package dsdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/url"
	"strings"
)

const jsonContentType = "application/json"

type DataPlaneApi struct {
	sdk *DataPlaneSDK
}

func NewDataPlaneApi(sdk *DataPlaneSDK) *DataPlaneApi {
	return &DataPlaneApi{sdk: sdk}
}

func (d *DataPlaneApi) Prepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	var prepareMessage DataFlowPrepareMessage

	if err := json.NewDecoder(r.Body).Decode(&prepareMessage); err != nil {
		d.decodeError(w, err)
		return
	}

	response, err := d.sdk.Prepare(r.Context(), prepareMessage)
	if err != nil {
		d.processError(w)
		return
	}

	var code int
	if response.State == Prepared {
		code = http.StatusOK
	} else {
		code = http.StatusAccepted
	}
	d.writeResponse(w, code, response)
}

func (d *DataPlaneApi) Start(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	var startMessage DataFlowStartMessage

	if err := json.NewDecoder(r.Body).Decode(&startMessage); err != nil {
		d.decodeError(w, err)
		return
	}

	response, err := d.sdk.Start(r.Context(), startMessage)
	if err != nil {
		d.processError(w)
		return
	}

	var code int
	if response.State == Started {
		code = http.StatusOK
	} else {
		code = http.StatusAccepted
	}
	d.writeResponse(w, code, response)

}

func (d *DataPlaneApi) Terminate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}

	processID, err := ParseIDFromURL(r.URL)
	if err != nil {
		d.processError(w)
		return
	}
	var terminateMessage DataFlowTerminateMessage

	if err := json.NewDecoder(r.Body).Decode(&terminateMessage); err != nil {
		d.decodeError(w, err)
		return
	}

	err = d.sdk.Terminate(r.Context(), processID)
	if err != nil {
		d.processError(w)
		return
	}

	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(http.StatusOK)
}

func (d *DataPlaneApi) decodeError(w http.ResponseWriter, err error) {
	id := uuid.NewString()
	d.sdk.Monitor.Printf("Error decoding flow [%s]: %v\n", id, err)
	d.writeResponse(w, http.StatusBadRequest, &DataFlowResponseMessage{Error: fmt.Sprintf("Failed to decode request body [%s]", id)})
}

func (d *DataPlaneApi) processError(w http.ResponseWriter) {
	id := uuid.NewString()
	message := fmt.Sprintf("Error processing flow [%s]", id)
	d.sdk.Monitor.Println(message)
	d.writeResponse(w, http.StatusInternalServerError, &DataFlowResponseMessage{Error: message})
}

func (d *DataPlaneApi) writeResponse(w http.ResponseWriter, code int, response *DataFlowResponseMessage) {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		id := uuid.NewString()
		message := fmt.Sprintf("Error encoding response [%s]", id)
		d.sdk.Monitor.Println(message)
		d.writeResponse(w, http.StatusInternalServerError, &DataFlowResponseMessage{Error: message})
		return
	}
}

func ParseIDFromURL(u *url.URL) (string, error) {
	if u == nil {
		return "", errors.New("URL cannot be nil")
	}

	path := u.Path
	if path == "" {
		return "", errors.New("URL path is empty")
	}

	// Remove trailing slash if present
	path = strings.TrimSuffix(path, "/")

	// Split the path by '/' to get path segments
	pathParts := strings.Split(path, "/")

	// Find the last non-empty segment
	for i := len(pathParts) - 1; i >= 0; i-- {
		if pathParts[i] != "" {
			return pathParts[i], nil
		}
	}

	return "", errors.New("no valid ID found in URL path")
}
