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

package functionaltest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func retirementActor(t *testing.T, tc *testContext, ctx context.Context) (*ateapipb.ObjectRef, *ateapipb.Worker) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "retirement-actor"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	actor, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: actor.GetStatus().GetWorkerAssignment().GetWorker()})
	if err != nil {
		t.Fatal(err)
	}
	return ref, worker
}

func retirementReadback(t *testing.T, tc *testContext, ctx context.Context, before *ateapipb.Worker) *ateapipb.Worker {
	t.Helper()
	worker, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: before.GetMetadata().GetName()}})
	if err != nil {
		t.Fatal(err)
	}
	if worker.GetMetadata().GetUid() != before.GetMetadata().GetUid() || worker.GetWorkerPodUid() != before.GetWorkerPodUid() || worker.GetNodeName() != before.GetNodeName() {
		t.Fatal("worker incarnation changed")
	}
	return worker
}

func retirementRefusal(t *testing.T, tc *testContext, ctx context.Context, worker *ateapipb.Worker, reason string) {
	t.Helper()
	proof, err := tc.persistence.ProveEmptyDraining(ctx, worker)
	var refused *store.OwnershipRefusal
	if proof != nil || !errors.As(err, &refused) || refused.Reason != reason {
		t.Fatalf("proof=%v err=%v, want %s", proof, err, reason)
	}
}

// Real PG, production RPC/Bind/Pause/upload composition. The native server
// models checkpoint bytes and completion; it never supplies proof decisions.
func TestWorkerRetirement_PauseUploadPositive(t *testing.T) {
	ns := namespaceForTest("ns-retirement-upload")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ref, worker := retirementActor(t, tc, ctx)
	if _, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("running worker drained: %v", err)
	}
	if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	worker = retirementReadback(t, tc, ctx, worker)
	// Deliberately discard the drain result, then use exact-incarnation
	// readback as after a lost ACK; never issue a broad name-only drain.
	if _, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()}); err != nil {
		t.Fatal(err)
	}
	drained := retirementReadback(t, tc, ctx, worker)
	if drained.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING || drained.GetMetadata().GetVersion() != worker.GetMetadata().GetVersion()+1 {
		t.Fatal("lost ACK readback was not the exact drain")
	}
	retirementRefusal(t, tc, ctx, drained, store.OwnershipLocalCheckpoint)
	entered, release := make(chan struct{}), make(chan struct{})
	tc.fakeAtelet.BeforeUpload = func(ctx context.Context, _ *ateletpb.UploadPausedCheckpointRequest) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	result := make(chan error, 1)
	go func() {
		_, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	retirementRefusal(t, tc, ctx, drained, store.OwnershipOperationPending)
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	actor, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || actor.GetStatus().GetPendingNativeSnapshot() != nil || actor.GetStatus().GetLocalSnapshot() != nil || actor.GetStatus().GetExternalSnapshot().GetSnapshotUri() == "" {
		t.Fatalf("upload did not settle ownership: %v", actor)
	}
	proof, err := tc.client.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained})
	if err != nil || !proto.Equal(proof.GetWorker(), drained) || proof.GetOwnershipGeneration() == "" {
		t.Fatalf("last worker positive missing: proof=%v err=%v", proof, err)
	}
	repeated, err := tc.client.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained})
	if err != nil || !proto.Equal(proof, repeated) {
		t.Fatalf("unchanged observations changed evidence: %v %v", repeated, err)
	}
}

func TestWorkerRetirement_AmbiguousCheckpointOrigin(t *testing.T) {
	for _, afterWork := range []bool{false, true} {
		name := "before"
		if afterWork {
			name = "after"
		}
		t.Run(name, func(t *testing.T) {
			ns := namespaceForTest("ns-retirement-error-" + name)
			tc := setupTest(t, ns)
			defer tc.cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			createTemplate(t, tc, ns)
			createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			ref, worker := retirementActor(t, tc, ctx)
			lost := status.Error(codes.Unavailable, "checkpoint completion unknown")
			if afterWork {
				tc.fakeAtelet.FailCheckpointAfterWork = lost
			} else {
				tc.fakeAtelet.FailCheckpoint = lost
			}
			if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err == nil {
				t.Fatal("ambiguous checkpoint reported success")
			}
			crashed, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			origin := crashed.GetStatus().GetPendingNativeSnapshot()
			if crashed.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED || crashed.GetStatus().GetWorkerAssignment() != nil || origin == nil || !proto.Equal(origin.GetSourceWorker(), worker) || origin.GetOperation() != "checkpoint-local" || origin.GetLocalSnapshotName() != crashed.GetStatus().GetInProgressLocalSnapshotName() {
				t.Fatalf("crash lost dispatch origin: %v", crashed)
			}
			tc.fakeAtelet.Lock.Lock()
			accepted := tc.fakeAtelet.LocalCheckpoints[origin.GetLocalSnapshotName()]
			tc.fakeAtelet.Lock.Unlock()
			if accepted != afterWork {
				t.Fatalf("local work accepted=%v, want %v", accepted, afterWork)
			}
			worker = retirementReadback(t, tc, ctx, worker)
			if worker.GetStatus().GetAllocated().GetActors() != 0 {
				t.Fatal("origin consumed a scheduling slot")
			}
			drained, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()})
			if err != nil {
				t.Fatal(err)
			}
			retirementRefusal(t, tc, ctx, drained, store.OwnershipOperationPending)
			// Reconstruct the control service with no workflow/cache state. Its
			// only source of operation origin is the persisted actor record.
			restarted := controlapi.NewRPCService(tc.persistence, nil, nil, nil, nil, nil, nil, "", 10*time.Second, nil, nil, "", nil, nil)
			readback, err := restarted.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err != nil || !proto.Equal(readback, crashed) {
				t.Fatalf("controller reconstruction lost operation origin: %v %v", readback, err)
			}
			if proof, err := restarted.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained}); proof != nil || status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("restarted producer forgot uncertain work: %v %v", proof, err)
			}
			if _, err := restarted.RevertActor(ctx, &ateapipb.RevertActorRequest{Actor: ref}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("revert erased uncertain work: %v", err)
			}
			if _, err := restarted.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("delete erased uncertain work: %v", err)
			}
			if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("crashed actor resumed: %v", err)
			}
			again, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err != nil || !proto.Equal(again, crashed) {
				t.Fatalf("recovery forgot uncertain origin: %v %v", again, err)
			}
			retirementRefusal(t, tc, ctx, drained, store.OwnershipOperationPending)
		})
	}
}

// releaseHandoffStore stops after production reservation release, before the
// production finalizer can atomically publish LocalSnapshot/clear its pointer.
type releaseHandoffStore struct {
	store.Interface
	released chan struct{}
	publish  chan struct{}
	once     sync.Once
}

func (s *releaseHandoffStore) ReleaseActorFromWorker(ctx context.Context, worker, actorUID string) (*ateapipb.Worker, error) {
	result, err := s.Interface.ReleaseActorFromWorker(ctx, worker, actorUID)
	if err != nil {
		return nil, err
	}
	s.once.Do(func() { close(s.released) })
	select {
	case <-s.publish:
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestWorkerRetirement_ProductionPauseHandoff(t *testing.T) {
	ns := namespaceForTest("ns-retirement-handoff")
	barrier := &releaseHandoffStore{released: make(chan struct{}), publish: make(chan struct{})}
	tc := setupTestWithWrappedStore(t, ns, nil, func(p store.Interface) store.Interface { barrier.Interface = p; return barrier })
	defer tc.cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var publishOnce sync.Once
	publish := func() { publishOnce.Do(func() { close(barrier.publish) }) }
	defer publish()
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	ref, worker := retirementActor(t, tc, ctx)
	result := make(chan error, 1)
	go func() { _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); result <- err }()
	select {
	case <-barrier.released:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	worker = retirementReadback(t, tc, ctx, worker)
	if worker.GetStatus().GetAllocated().GetActors() != 0 {
		t.Fatal("barrier did not observe production release")
	}
	actor, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	if actor.GetStatus().GetWorkerAssignment() == nil || actor.GetStatus().GetPendingNativeSnapshot() == nil || actor.GetStatus().GetLocalSnapshot() != nil {
		t.Fatalf("handoff lost the pre-publication pointer/origin: %v", actor)
	}
	if _, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("empty allocation hid pending actor pointer: %v", err)
	}
	publish()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	actor, err = tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		t.Fatal(err)
	}
	if actor.GetStatus().GetWorkerAssignment() != nil || actor.GetStatus().GetPendingNativeSnapshot() != nil || actor.GetStatus().GetLocalSnapshot().GetSnapshotName() == "" {
		t.Fatalf("atomic publication did not conserve local ownership: %v", actor)
	}
	worker = retirementReadback(t, tc, ctx, worker)
	drained, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()})
	if err != nil {
		t.Fatal(err)
	}
	retirementRefusal(t, tc, ctx, drained, store.OwnershipLocalCheckpoint)
}
