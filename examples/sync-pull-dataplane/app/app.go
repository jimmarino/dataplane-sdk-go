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

package app

import (
	"context"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
)

type ClientApp struct {
	api *dsdk.DataPlaneApi
}

func NewClientApp() *ClientApp {
	return &ClientApp{}
}

func (*ClientApp) init() {

}

func (a *ClientApp) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, duplicate bool, sdk *dsdk.DataPlaneSDK) (*dsdk.DataFlowResponseMessage, error) {
	return &dsdk.DataFlowResponseMessage{State: dsdk.Prepared}, nil
}

func (a *ClientApp) startProcessor(ctx context.Context, flow *dsdk.DataFlow, duplicate bool, sdk *dsdk.DataPlaneSDK) (*dsdk.DataFlowResponseMessage, error) {
	return &dsdk.DataFlowResponseMessage{State: dsdk.Started}, nil
}
