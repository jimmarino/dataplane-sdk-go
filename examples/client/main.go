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
	"github.com/metaform/dataplane-sdk-go/examples/client/controlplane"
)

// Example usage
func main() {
	ctx := context.Background()

	if err := controlplane.DataFlowStart(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
