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
		{Crash: &ateapipb.ActorCrash{Message: "pause failed: worker pod went away while hosting the actor"}},
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
	if status.Code(err) != codes.FailedPrecondition || repeated != nil {
		t.Fatalf("same digest authorized unknown operation replay: %v %v", repeated, err)
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

func TestAcknowledgedTerminationRetainsExactOrigin(t *testing.T) {
	worker := validWorker(apiWorkerName)
	worker.Metadata.Uid = apiWorkerName
	worker.Metadata.Version = 1
	assignment := workerAssignmentFrom(worker)
	actor := &ateapipb.Actor{Status: &ateapipb.ActorStatus{
		State:            ateapipb.ActorState_ACTOR_STATE_DELETING,
		WorkerAssignment: proto.CloneOf(assignment),
		PendingNativeOperation: &ateapipb.NativeOperationOwnership{
			Operation:     terminatedAcknowledged,
			RequestDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			SourceWorker:  worker,
			Assignment:    assignment,
		},
	}}
	if err := requireSettledNativeOperation(actor); err != nil {
		t.Fatalf("positive ACK blocked cleanup: %v", err)
	}
	for _, mutate := range []func(*ateapipb.Actor){
		func(a *ateapipb.Actor) { a.Status.PendingNativeOperation.Operation = "terminate" },
		func(a *ateapipb.Actor) { a.Status.PendingNativeOperation.RequestDigest = "unknown" },
		func(a *ateapipb.Actor) { a.Status.PendingNativeOperation.SourceWorker.Metadata.Uid = "" },
		func(a *ateapipb.Actor) { a.Status.PendingNativeOperation.SourceWorker.Metadata.Version = 0 },
		func(a *ateapipb.Actor) { a.Status.PendingNativeOperation.Assignment.WorkerPodUid = "replaced" },
		func(a *ateapipb.Actor) { a.Status.WorkerAssignment.NodeName = "another-node" },
		func(a *ateapipb.Actor) { a.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING },
	} {
		changed := proto.CloneOf(actor)
		mutate(changed)
		if err := requireSettledNativeOperation(changed); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("changed/unknown termination accepted: %v %v", changed, err)
		}
	}
	// Release may already have committed before later cleanup fails. The
	// positive envelope retains the original coordinates independently.
	actor.Status.WorkerAssignment = nil
	if err := requireSettledNativeOperation(actor); err != nil {
		t.Fatalf("released assignment erased positive ACK: %v", err)
	}
	actor.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
	if err := requireSettledNativeOperation(actor); err != nil {
		t.Fatalf("worker-loss crash erased positive ACK: %v", err)
	}
}

func TestCompletedNativeInvocationRequiresExactTerminalEvidence(t *testing.T) {
	invocation := &ateletpb.ExecuteOperationRequest{OperationId: "persisted-operation-id", WorkerUid: "worker-uid", WorkerPodUid: "worker-pod-uid", Request: &ateletpb.ExecuteOperationRequest_Run{Run: &ateletpb.RunRequest{ActorUid: "actor-uid", TargetAteomUid: "worker-pod-uid"}}}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(invocation)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	success := &ateletpb.OperationResult{OperationId: invocation.OperationId, RequestSha256: hex.EncodeToString(h[:]), State: ateletpb.OperationState_OPERATION_STATE_SUCCEEDED}
	if err := completedNativeInvocation(invocation, success); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ateletpb.OperationResult){
		func(r *ateletpb.OperationResult) { r.OperationId = "another-operation" },
		func(r *ateletpb.OperationResult) { r.RequestSha256 = "another-request" },
		func(r *ateletpb.OperationResult) { r.State = ateletpb.OperationState_OPERATION_STATE_MISSING },
		func(r *ateletpb.OperationResult) { r.State = ateletpb.OperationState_OPERATION_STATE_UNRESOLVED },
		func(r *ateletpb.OperationResult) { r.State = ateletpb.OperationState_OPERATION_STATE_UNSPECIFIED },
		func(r *ateletpb.OperationResult) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		r := proto.CloneOf(success)
		change(r)
		if err := completedNativeInvocation(invocation, r); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("inexact terminal evidence accepted: %v %v", r, err)
		}
	}
	if err := completedNativeInvocation(invocation, nil); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("nil receipt: %v", err)
	}
}
