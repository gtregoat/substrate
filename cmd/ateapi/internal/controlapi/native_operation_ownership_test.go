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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRequireSettledNativeOperation(t *testing.T) {
	for _, s := range []*ateapipb.ActorStatus{
		{PendingNativeOperation: &ateapipb.NativeOperationOwnership{Operation: "checkpoint-local"}},
		{InProgressLocalSnapshotName: "legacy-unlocated"},
		{InProgressSnapshotUri: "gs://legacy/unknown"},
		{Crash: &ateapipb.ActorCrash{Message: "resume failed: atelet Restore: completion unknown"}},
	} {
		if err := requireSettledNativeOperation(&ateapipb.Actor{Status: s}); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("unresolved origin allowed recovery: %v", err)
		}
	}
	if err := requireSettledNativeOperation(&ateapipb.Actor{Status: &ateapipb.ActorStatus{
		ExternalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://retained/external"},
		LocalSnapshot:    &ateapipb.LocalSnapshot{SnapshotName: "settled-local", NodeVmsWithLocalSnapshots: []string{"node"}},
	}}); err != nil {
		t.Fatalf("settled history prevented legitimate recovery: %v", err)
	}
}

func TestRecordNativeOperationOrigin_IdentityAndRetry(t *testing.T) {
	p := newTestPersistence(t)
	ctx := context.Background()
	worker, err := p.CreateWorker(ctx, validWorker(apiWorkerName, withWorkerStatus(func(s *ateapipb.WorkerStatus) { s.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE })))
	if err != nil {
		t.Fatal(err)
	}
	actor := storetest.MustCreateActor(t, ctx, p, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "origin"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSING, InProgressLocalSnapshotName: "pending", WorkerAssignment: workerAssignmentFrom(worker)},
	})
	ref := resources.ActorRefFromActor(actor)
	if err := p.BindActorToWorker(ctx, worker.GetMetadata().GetName(), &ateapipb.ActorAssignment{Actor: &ateapipb.ObjectRef{Atespace: ref.Atespace, Name: ref.Name}, ActorUid: actor.GetMetadata().GetUid()}, nil); err != nil {
		t.Fatal(err)
	}
	worker, err = p.GetWorker(ctx, worker.GetMetadata().GetName())
	if err != nil {
		t.Fatal(err)
	}
	workflow := &ActorWorkflow{store: p}
	request := &ateletpb.CheckpointRequest{ActorUid: actor.GetMetadata().GetUid(), TargetAteomUid: worker.GetWorkerPodUid(), Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL, Config: &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: "pending"}}}
	recorded, err := workflow.recordNativeOperationOrigin(ctx, ref, actor, "checkpoint-local", request)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(recorded.GetStatus().GetPendingNativeOperation().GetSourceWorker(), worker) || len(recorded.GetStatus().GetPendingNativeOperation().GetRequestDigest()) != 64 {
		t.Fatal("origin was not pinned to the actual worker/request")
	}
	repeated, err := workflow.recordNativeOperationOrigin(ctx, ref, recorded, "checkpoint-local", request)
	if err != nil || !proto.Equal(repeated, recorded) {
		t.Fatalf("same operation retry changed origin/version: %v %v", repeated, err)
	}
	changed := proto.CloneOf(request)
	changed.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
	if _, err := workflow.recordNativeOperationOrigin(ctx, ref, recorded, "checkpoint-local", changed); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("changed pending request accepted: %v", err)
	}
	if _, err := p.DeleteWorker(ctx, worker.GetMetadata().GetName(), store.DeletePreconditions{UID: worker.GetMetadata().GetUid()}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CreateWorker(ctx, validWorker(apiWorkerName)); err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.recordNativeOperationOrigin(ctx, ref, recorded, "checkpoint-local", request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("worker name/Pod mapping reuse accepted a new incarnation: %v", err)
	}
	after, err := p.GetActor(ctx, ref)
	if err != nil || !proto.Equal(after, recorded) {
		t.Fatalf("refusals changed recorded origin: %v %v", after, err)
	}
}
