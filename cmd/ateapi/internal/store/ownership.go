// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package store contains common types for the persistence layer.
package store

import "fmt"

// OwnershipRefusal is a concrete failure to establish retirement eligibility.
// No response or partial observations may be used as ownership evidence.
type OwnershipRefusal struct {
 Reason string
}

func (e *OwnershipRefusal) Error() string { return fmt.Sprintf("retirement ownership refused: %s", e.Reason) }
func (e *OwnershipRefusal) Unwrap() error { return ErrFailedPrecondition }

const (
 OwnershipSchemaUnavailable = "OWNERSHIP_SCHEMA_UNAVAILABLE"
 OwnershipMappingChanged = "OWNERSHIP_MAPPING_CHANGED"
 OwnershipNotDraining = "OWNERSHIP_NOT_DRAINING"
 OwnershipBusy = "OWNERSHIP_BUSY"
 OwnershipLocalCheckpoint = "OWNERSHIP_LOCAL_CHECKPOINT"
 OwnershipOperationPending = "OWNERSHIP_OPERATION_PENDING"
 OwnershipUnknown = "OWNERSHIP_UNKNOWN"
 OwnershipNodeRetired = "OWNERSHIP_NODE_RETIRED"
)
