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
	"sync"
)

// CopyFunc defines a function type for copying values
type CopyFunc[T any] func(T) T

// DefaultCopyFunc provides a default copy implementation using Go's assignment behavior
func DefaultCopyFunc[T any](value T) T {
	return value
}

// PredicateFunc defines a function type for filtering values
type PredicateFunc[T any] func(key string, value T) bool

// Store persists generic values in memory.
type Store[T any] struct {
	mu       sync.RWMutex
	items    map[string]T
	copyFunc CopyFunc[T]
}

// NewStore creates a new Store instance with default copy behavior
func NewStore[T any]() *Store[T] {
	return &Store[T]{
		items:    make(map[string]T),
		copyFunc: DefaultCopyFunc[T],
	}
}

// NewStoreWithCopyFunc creates a new Store instance with a custom copy function
func NewStoreWithCopyFunc[T any](copyFunc CopyFunc[T]) *Store[T] {
	return &Store[T]{
		items:    make(map[string]T),
		copyFunc: copyFunc,
	}
}

// Create adds a new item to the store with the given key
func (s *Store[T]) Create(key string, value T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = s.copyFunc(value)
}

// Update modifies an existing item in the store
func (s *Store[T]) Update(key string, value T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[key]; exists {
		s.items[key] = s.copyFunc(value)
		return true
	}
	return false
}

// Delete removes an item from the store
func (s *Store[T]) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[key]; exists {
		delete(s.items, key)
		return true
	}
	return false
}

// Find retrieves an item from the store, returning a copy
func (s *Store[T]) Find(key string) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.items[key]
	if exists {
		return s.copyFunc(value), true
	}

	var zero T
	return zero, false
}

// FindFirst searches for the first item in the store that matches the given predicate
// Returns the key, value, and a boolean indicating if a match was found
func (s *Store[T]) FindFirst(predicate PredicateFunc[T]) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, value := range s.items {
		if predicate(key, value) {
			return s.copyFunc(value), true
		}
	}

	var zero T
	return zero, false
}

func (s *Store[T]) FindAndDelete(predicate PredicateFunc[T]) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, value := range s.items {
		if predicate(key, value) {
			val := s.copyFunc(value)
			delete(s.items, key)
			return val, true
		}
	}
	var zero T
	return zero, false
}

// Has checks if an item exists in the store
func (s *Store[T]) Has(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.items[key]
	return exists
}
