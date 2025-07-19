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
	"testing"
	"time"
)

func TestDataFlow_transitionToPreparing(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    false,
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 50 to Preparing",
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 100 to Preparing",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 150 to Preparing",
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 200 to Preparing",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 250 to Preparing",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 300 to Preparing",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 350 to Preparing",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 5,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToPreparing()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error, expected %v, got %v", tc.initialState, df.State)
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error, expected %v, got %v", initialStateCount, df.StateCount)
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Preparing {
					t.Errorf("expected state %v, got %v", Preparing, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

func TestDataFlow_transitionToPrepared(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    false,
		},
		"valid transition from Preparing": {
			initialState: Preparing,
			expectErr:    false,
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 100 to Prepared",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 150 to Prepared",
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 200 to Prepared",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 250 to Prepared",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 300 to Prepared",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 350 to Prepared",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 3,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToPrepared()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Prepared {
					t.Errorf("expected state %v, got %v", Prepared, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

func TestDataFlow_transitionToStarting(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    false,
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 50 to Starting",
		},
		"valid transition from Prepared": {
			initialState: Prepared,
			expectErr:    false,
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 150 to Starting",
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 200 to Starting",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 250 to Starting",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 300 to Starting",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 350 to Starting",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 2,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToStarting()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Starting {
					t.Errorf("expected state %v, got %v", Starting, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

func TestDataFlow_transitionToStarted(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    false,
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 50 to Started",
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 100 to Started",
		},
		"valid transition from Starting": {
			initialState: Starting,
			expectErr:    false,
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 200 to Started",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 250 to Started",
		},
		"valid transition from Suspended": {
			initialState: Suspended,
			expectErr:    false,
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 350 to Started",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 1,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToStarted()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Started {
					t.Errorf("expected state %v, got %v", Started, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

func TestDataFlow_transitionToSuspended(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"invalid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 0 to Suspended",
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 50 to Suspended",
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 100 to Suspended",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 150 to Suspended",
		},
		"valid transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 250 to Suspended",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 300 to Suspended",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 350 to Suspended",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 4,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToSuspended()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Suspended {
					t.Errorf("expected state %v, got %v", Suspended, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

func TestDataFlow_transitionToCompleted(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"invalid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 0 to Completed",
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 50 to Completed",
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 100 to Completed",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 150 to Completed",
		},
		"valid transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 250 to Completed",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 300 to Completed",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from 350 to Completed",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 7,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToCompleted()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Completed {
					t.Errorf("expected state %v, got %v", Completed, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

func TestDataFlow_transitionToTerminated(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
	}{
		"valid transition from Uninitialized": {
			initialState: Uninitialized,
			expectErr:    false,
		},
		"valid transition from Preparing": {
			initialState: Preparing,
			expectErr:    false,
		},
		"valid transition from Prepared": {
			initialState: Prepared,
			expectErr:    false,
		},
		"valid transition from Starting": {
			initialState: Starting,
			expectErr:    false,
		},
		"valid transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"valid transition from Completed": {
			initialState: Completed,
			expectErr:    false,
		},
		"valid transition from Suspended": {
			initialState: Suspended,
			expectErr:    false,
		},
		"valid transition from Terminated": {
			initialState: Terminated,
			expectErr:    false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 10,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.transitionToTerminated()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Terminated {
					t.Errorf("expected state %v, got %v", Terminated, df.State)
				}
				if df.StateCount != initialStateCount+1 {
					t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
				}
				if df.StateTimestamp <= initialTimestamp {
					t.Errorf("state timestamp should be updated")
				}
			}
		})
	}
}

// Test to ensure timestamps are properly set and state counts increment correctly
func TestDataFlow_TransitionsTimestampAndCounter(t *testing.T) {
	df := &DataFlow{
		State:      Uninitialized,
		StateCount: 0,
	}

	// Capture time before transition
	timeBefore := time.Now().UnixMilli()

	// Perform a valid transition
	err := df.transitionToPreparing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Capture time after transition
	timeAfter := time.Now().UnixMilli()

	// Verify timestamp is within reasonable range
	if df.StateTimestamp < timeBefore || df.StateTimestamp > timeAfter {
		t.Errorf("timestamp %d should be between %d and %d", df.StateTimestamp, timeBefore, timeAfter)
	}

	// Verify state count incremented
	if df.StateCount != 1 {
		t.Errorf("expected state count 1, got %d", df.StateCount)
	}

	// Test multiple transitions to ensure counter keeps incrementing
	err = df.transitionToPrepared()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if df.StateCount != 2 {
		t.Errorf("expected state count 2, got %d", df.StateCount)
	}
}

// Test case with zero initial timestamp
func TestDataFlow_TransitionFromZeroTimestamp(t *testing.T) {
	df := &DataFlow{
		State:          Uninitialized,
		StateCount:     0,
		StateTimestamp: 0,
	}

	err := df.transitionToPreparing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should update timestamp from 0 to current time
	if df.StateTimestamp <= 0 {
		t.Errorf("expected positive timestamp, got %d", df.StateTimestamp)
	}

	// Should increment counter from 0 to 1
	if df.StateCount != 1 {
		t.Errorf("expected state count 1, got %d", df.StateCount)
	}
}
