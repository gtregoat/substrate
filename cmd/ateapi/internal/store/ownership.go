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

import (
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"strings"
)

// OwnershipRefusal identifies a missing native fact. It never carries partial
// success. Consumers must keep protection for every refusal, including unknown
// reasons from newer servers.
type OwnershipRefusal struct{ Reason string }

func (e *OwnershipRefusal) Error() string { return "worker ownership proof refused: " + e.Reason }
func (e *OwnershipRefusal) Unwrap() error { return ErrFailedPrecondition }

const (
	OwnershipMappingChanged    = "OWNERSHIP_MAPPING_CHANGED"
	OwnershipNotDraining       = "OWNERSHIP_NOT_DRAINING"
	OwnershipBusy              = "OWNERSHIP_BUSY"
	OwnershipLocalCheckpoint   = "OWNERSHIP_LOCAL_CHECKPOINT"
	OwnershipOperationPending  = "OWNERSHIP_OPERATION_PENDING"
	OwnershipUnknown           = "OWNERSHIP_UNKNOWN"
	OwnershipStabilityUnproven = "OWNERSHIP_STABILITY_UNPROVEN"
)

// UnlocatedNativeFailure recognizes legacy failure envelopes only to refuse
// evidence/recovery, never to fabricate a worker/node attribution or settlement.
// Pod loss during a lifecycle step could hide accepted work even when an older
// writer erased its local identifier. A qualified persisted origin scopes it.
func UnlocatedNativeFailure(s *ateapipb.ActorStatus) bool {
	if s.GetPendingNativeOperation() != nil {
		return false
	}
	message := s.GetCrash().GetMessage()
	if strings.Contains(message, "atelet ") {
		return true
	}
	for _, prefix := range []string{"resume failed:", "pause failed:", "suspend failed:", "revert failed:"} {
		if strings.HasPrefix(message, prefix) {
			return true
		}
	}
	return false
}
