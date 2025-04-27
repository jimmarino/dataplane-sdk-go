# Go Dataplane SDK

A Dataplane SDK for Go. This SDK provides components for creating Go-based dataplanes that interface with Dataspace
Protocol Control Planes via the `Dataplane Signalling API`. The SDK includes state management, support for reliable
qualities of service, recovery, and error handling.

## Main Operations

### 1. Provision

- Purpose: Provisions an endpoint for receiving data
- Function: `Provision(ctx context.Context, message DataFlowProvisionMessage) (*DataFlowResponseMessage, error)`
- Returns: Response message or error

### 2. Start

- Purpose: Initiates a data flow on the provider side
- Function: `Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error)`
- Takes: DataFlowStartMessage as input

### 3. Terminate

- Purpose: Ends a data flow
- Function: `Terminate(ctx context.Context, processId string) error`
- Requires: Process ID

### 4. Suspend

- Purpose: Temporarily halts a data flow
- Function: `Suspend(ctx context.Context, processId string) error`
- Requires: Process ID

### 5. Recover

- Purpose: Handles recovery of data flows
- Function: `Recover(ctx context.Context) error`
- Processes multiple flows that need recovery

## Key Features

### State Management

- Maintains different states for data flows:
    - Provisioning
    - Started
    - Terminated
    - Suspended

### Built-in Capabilities

- Deduplication logic for handling duplicate messages
- Transaction support via TransactionContext
- Comprehensive error handling and propagation
- Extension points through callback functions

## Extension Points

- : Custom provisioning logic `OnProvision`
- : Custom start logic `OnStart`
- : Custom termination logic `OnTerminate`
- : Custom suspension logic `OnSuspend`
- : Custom recovery logic `OnRecover`

## Usage Example

``` go
sdk := DataPlaneSDK{
    Store: myStore,
    TrxContext: myTrxContext,
    OnProvision: myProvisionHandler,
    OnStart: myStartHandler,
    OnTerminate: myTerminateHandler,
    OnSuspend: mySuspendHandler,
    OnRecover: myRecoverHandler,
}
```
