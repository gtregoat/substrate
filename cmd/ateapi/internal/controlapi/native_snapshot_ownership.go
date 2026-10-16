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
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recordNativeSnapshotOrigin runs before dispatch, while the actor still owns
// its assignment or local snapshot. Releasing a reservation does not clear it.
// The actor CAS prevents a concurrent crash or replacement from losing origin.
func (w *ActorWorkflow) recordNativeSnapshotOrigin(ctx context.Context, ref resources.ActorRef, actor *ateapipb.Actor, operation string) (*ateapipb.Actor, error) {
	s := actor.GetStatus()
	origin := &ateapipb.NativeSnapshotOperation{
		Operation:         operation,
		LocalSnapshotName: s.GetInProgressLocalSnapshotName(),
		SnapshotUri:       s.GetInProgressSnapshotUri(),
	}
	switch operation {
	case "checkpoint-local", "checkpoint-external":
		a := s.GetWorkerAssignment()
		if a == nil || a.GetWorker().GetName() == "" || a.GetWorkerPodUid() == "" || a.GetNodeName() == "" {
			return nil, status.Error(codes.FailedPrecondition, "native snapshot origin has no qualified assignment")
		}
		worker, err := w.store.GetWorker(ctx, a.GetWorker().GetName())
		if err != nil {
			return nil, fmt.Errorf("while recording native snapshot origin: %w", err)
		}
		if !proto.Equal(a, workerAssignmentFrom(worker)) || worker.GetMetadata().GetUid() == "" || worker.GetMetadata().GetVersion() < 1 {
			return nil, status.Error(codes.FailedPrecondition, "native snapshot origin worker mapping changed")
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
		return nil, status.Error(codes.FailedPrecondition, "unknown native snapshot operation")
	}
	if existing := s.GetPendingNativeSnapshot(); existing != nil {
		// Allocation/version may have changed since dispatch. Preserve the first
		// observation, but never adopt a different incarnation or operation.
		if existing.GetSourceWorker().GetMetadata().GetUid() != origin.GetSourceWorker().GetMetadata().GetUid() {
			return nil, status.Error(codes.FailedPrecondition, "unsettled native snapshot worker incarnation changed")
		}
		origin.SourceWorker = existing.GetSourceWorker()
		if !proto.Equal(existing, origin) {
			return nil, status.Error(codes.FailedPrecondition, "unsettled native snapshot origin changed")
		}
		return actor, nil
	}
	return w.store.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
		a.Status.PendingNativeSnapshot = origin
		return nil
	})
}

// Unknown native completion cannot be settled by deleting an external URI or
// by discarding actor status. Recovery keeps the record until a separately
// evidenced native settlement is available, including for unlocated legacy work.
func requireSettledNativeSnapshot(actor *ateapipb.Actor) error {
	s := actor.GetStatus()
	if s.GetPendingNativeSnapshot() != nil || s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "" {
		return status.Error(codes.FailedPrecondition, "native snapshot completion is unresolved; retain operation ownership")
	}
	return nil
}
