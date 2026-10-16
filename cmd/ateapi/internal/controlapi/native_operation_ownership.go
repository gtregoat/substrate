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

package controlapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recordNativeOperationOrigin runs before dispatch, while the actor still owns
// its assignment or local snapshot. Releasing a reservation does not clear it.
// The actor CAS prevents a concurrent crash or replacement from losing origin.
func (w *ActorWorkflow) recordNativeOperationOrigin(ctx context.Context, ref resources.ActorRef, actor *ateapipb.Actor, operation string, request proto.Message) (*ateapipb.Actor, error) {
	state := actor.GetStatus().GetState()
	validState := operation == "checkpoint-local" && state == ateapipb.ActorState_ACTOR_STATE_PAUSING ||
		(operation == "checkpoint-external" || operation == "upload-local") && state == ateapipb.ActorState_ACTOR_STATE_SUSPENDING ||
		(operation == "run" || operation == "restore") && state == ateapipb.ActorState_ACTOR_STATE_RESUMING ||
		operation == "terminate" && (state == ateapipb.ActorState_ACTOR_STATE_DELETING || state == ateapipb.ActorState_ACTOR_STATE_REVERTING)
	if !validState {
		return nil, status.Error(codes.FailedPrecondition, "native operation does not match actor lifecycle state")
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encoding native operation request: %w", err)
	}
	digest := sha256.Sum256(data)
	s := actor.GetStatus()
	origin := &ateapipb.NativeOperationOwnership{
		Operation:         operation,
		RequestDigest:     hex.EncodeToString(digest[:]),
		LocalSnapshotName: s.GetInProgressLocalSnapshotName(),
		SnapshotUri:       s.GetInProgressSnapshotUri(),
	}
	switch operation {
	case "checkpoint-local", "checkpoint-external", "run", "restore", "terminate":
		a := s.GetWorkerAssignment()
		if a == nil || a.GetWorker().GetName() == "" || a.GetWorkerPodUid() == "" || a.GetNodeName() == "" {
			return nil, status.Error(codes.FailedPrecondition, "native operation origin has no qualified assignment")
		}
		worker, err := w.store.GetWorker(ctx, a.GetWorker().GetName())
		if err != nil {
			return nil, fmt.Errorf("while recording native operation origin: %w", err)
		}
		if !proto.Equal(a, workerAssignmentFrom(worker)) || worker.GetMetadata().GetUid() == "" || worker.GetMetadata().GetVersion() < 1 {
			return nil, status.Error(codes.FailedPrecondition, "native operation origin worker mapping changed")
		}
		if s.GetPendingNativeOperation() == nil {
			claim, err := w.store.GetWorkerAssignment(ctx, a.GetWorker().GetName(), actor.GetMetadata().GetUid())
			if err != nil {
				return nil, fmt.Errorf("native operation origin has no durable claim: %w", err)
			}
			if claim.GetActorUid() != actor.GetMetadata().GetUid() || claim.GetActor().GetAtespace() != ref.Atespace || claim.GetActor().GetName() != ref.Name || worker.GetStatus().GetAllocated().GetActors() < 1 {
				return nil, status.Error(codes.FailedPrecondition, "native operation origin claim is inconsistent")
			}
		}
		origin.SourceWorker = proto.CloneOf(worker)
		origin.Assignment = proto.CloneOf(a)
	case "upload-local":
		local := s.GetLocalSnapshot()
		if local.GetSnapshotName() == "" || len(local.GetNodeVmsWithLocalSnapshots()) == 0 || s.GetWorkerAssignment() != nil {
			return nil, status.Error(codes.FailedPrecondition, "native upload origin has no qualified local snapshot")
		}
		origin.LocalSource = proto.CloneOf(local)
	default:
		return nil, status.Error(codes.FailedPrecondition, "unknown native operation")
	}
	if existing := s.GetPendingNativeOperation(); existing != nil {
		// Allocation/version may have changed since dispatch. Preserve the first
		// observation, but never adopt a different incarnation or operation.
		if existing.GetSourceWorker().GetMetadata().GetUid() != origin.GetSourceWorker().GetMetadata().GetUid() {
			return nil, status.Error(codes.FailedPrecondition, "unsettled native operation worker incarnation changed")
		}
		origin.SourceWorker = existing.GetSourceWorker()
		if !proto.Equal(existing, origin) {
			return nil, status.Error(codes.FailedPrecondition, "unsettled native operation origin changed")
		}
		return actor, nil
	}
	return w.store.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
		a.Status.PendingNativeOperation = origin
		return nil
	})
}

// Unknown native completion cannot be settled by deleting an external URI or
// by discarding actor status. Recovery keeps the record until a separately
// evidenced native settlement is available, including for unlocated legacy work.
func requireSettledNativeOperation(actor *ateapipb.Actor) error {
	s := actor.GetStatus()
	if (s.GetPendingNativeOperation() != nil && !acknowledgedNativeTermination(actor)) || store.UnlocatedNativeFailure(s) || s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "" {
		return status.Error(codes.FailedPrecondition, "native operation completion is unresolved; retain operation ownership")
	}
	return nil
}

// The operation envelope also retains a positive native ACK through retryable
// cleanup failures. Older readers conservatively refuse this operation string.
const terminatedAcknowledged = "terminate-acknowledged"

func acknowledgedNativeTermination(actor *ateapipb.Actor) bool {
	s := actor.GetStatus()
	op := s.GetPendingNativeOperation()
	if op.GetOperation() != terminatedAcknowledged ||
		(s.GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING && s.GetState() != ateapipb.ActorState_ACTOR_STATE_REVERTING && s.GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED) {
		return false
	}
	digest, err := hex.DecodeString(op.GetRequestDigest())
	return err == nil && len(digest) == sha256.Size && op.GetLocalSource() == nil &&
		op.GetLocalSnapshotName() == s.GetInProgressLocalSnapshotName() && op.GetSnapshotUri() == s.GetInProgressSnapshotUri() &&
		op.GetSourceWorker().GetMetadata().GetUid() != "" &&
		op.GetAssignment() != nil && proto.Equal(op.GetAssignment(), workerAssignmentFrom(op.GetSourceWorker())) &&
		(s.GetWorkerAssignment() == nil || proto.Equal(s.GetWorkerAssignment(), op.GetAssignment()))
}

// Called only after a successful native response. A bounded CAS reconciliation
// can use that knowledge within this call, but cannot replay an unknown RPC.
func (w *ActorWorkflow) settleNativeTermination(ctx context.Context, ref resources.ActorRef, actor *ateapipb.Actor) error {
	origin := actor.GetStatus().GetPendingNativeOperation()
	for attempt := 0; attempt < 2; attempt++ {
		_, err := w.store.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
			if origin.GetOperation() != "terminate" || !proto.Equal(a.GetStatus().GetPendingNativeOperation(), origin) {
				return status.Error(codes.FailedPrecondition, "termination operation changed before settlement")
			}
			a.Status.PendingNativeOperation = proto.CloneOf(origin)
			a.Status.PendingNativeOperation.Operation = terminatedAcknowledged
			return nil
		})
		if err == nil {
			return nil
		}
		latest, readErr := w.store.GetActor(ctx, ref)
		if readErr != nil {
			return errors.Join(err, readErr)
		}
		if latest.GetMetadata().GetUid() != actor.GetMetadata().GetUid() ||
			latest.GetStatus().GetState() != actor.GetStatus().GetState() ||
			!proto.Equal(latest.GetStatus().GetWorkerAssignment(), actor.GetStatus().GetWorkerAssignment()) {
			return err
		}
		expected := proto.CloneOf(origin)
		expected.Operation = terminatedAcknowledged
		if acknowledgedNativeTermination(latest) && proto.Equal(latest.GetStatus().GetPendingNativeOperation(), expected) {
			return nil // The settlement write committed but its response was lost.
		}
		if !errors.Is(err, store.ErrVersionConflict) || !proto.Equal(latest.GetStatus().GetPendingNativeOperation(), origin) {
			return err
		}
		actor = latest
	}
	return status.Error(codes.Aborted, "termination settlement conflicted; ownership retained")
}
