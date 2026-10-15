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

package atepg

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

func ownershipTestWorker() *ateapipb.Worker {
	return &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "proof-worker"}, WorkerNamespace: "ns", WorkerPool: "pool", WorkerPod: "pod", WorkerPodUid: "11111111-1111-4111-8111-111111111111", NodeName: "node", Ip: "10.0.0.1", Status: &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}}
}

func requireOwnershipRefusal(t *testing.T, proof *ateapipb.ProveEmptyDrainingResponse, err error, reason string) {
	t.Helper()
	var refused *store.OwnershipRefusal
	if proof != nil || !errors.As(err, &refused) || refused.Reason != reason || !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("proof=%v err=%v, want %s", proof, err, reason)
	}
}

func TestProveEmptyDraining_ProductionRefusalsAndNoEffects(t *testing.T) {
	for _, scenario := range []string{"empty", "active", "mapping", "local", "upload", "external", "unknown-worker", "unknown-actor", "corrupt-actor", "busy", "allocated", "actor-pointer", "unknown-metadata"} {
		t.Run(scenario, func(t *testing.T) {
			p := setupPostgresPersistence(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			createTestAtespace(t, p, "proof-team")
			worker, err := p.CreateWorker(ctx, ownershipTestWorker())
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "active" {
				worker, err = p.DrainEmptyWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(worker))
				if err != nil {
					t.Fatal(err)
				}
			}
			expected := proto.CloneOf(worker)
			reason := store.OwnershipStabilityUnproven
			switch scenario {
			case "active":
				reason = store.OwnershipNotDraining
			case "mapping":
				expected.WorkerPodUid = "22222222-2222-4222-8222-222222222222"
				reason = store.OwnershipMappingChanged
			case "unknown-worker":
				worker.Status.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
				data, err := proto.Marshal(worker)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := p.pool.Exec(ctx, `UPDATE workers SET proto = $1 WHERE name = $2`, data, worker.GetMetadata().GetName()); err != nil {
					t.Fatal(err)
				}
				reason = store.OwnershipUnknown
			case "allocated":
				worker, err = p.UpdateWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(worker), func(w *ateapipb.Worker) error { w.Status.Allocated = &ateapipb.WorkerResources{Actors: 1}; return nil })
				if err != nil {
					t.Fatal(err)
				}
				expected = proto.CloneOf(worker)
				reason = store.OwnershipBusy
			case "unknown-metadata":
				if _, err := p.pool.Exec(ctx, `UPDATE workers SET version = version + 1 WHERE name = $1`, worker.GetMetadata().GetName()); err != nil {
					t.Fatal(err)
				}
				reason = store.OwnershipUnknown
			case "busy":
				// Nil admission is intentionally the lower-level store path; no RPC or
				// scheduler safety is bypassed in a live environment.
				if err := p.BindActorToWorker(ctx, worker.GetMetadata().GetName(), &ateapipb.ActorAssignment{ActorUid: "owned-actor"}, nil); err != nil {
					t.Fatal(err)
				}
				expected, err = p.GetWorker(ctx, worker.GetMetadata().GetName())
				if err != nil {
					t.Fatal(err)
				}
				reason = store.OwnershipBusy
			}
			if scenario == "local" || scenario == "upload" || scenario == "external" || scenario == "unknown-actor" || scenario == "corrupt-actor" || scenario == "actor-pointer" {
				a := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "proof-team", Name: "owned"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ExternalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://retained/actor/snapshot"}}}
				if scenario == "local" || scenario == "upload" {
					a.Status.State = ateapipb.ActorState_ACTOR_STATE_PAUSED
					a.Status.LocalSnapshot = &ateapipb.LocalSnapshot{SnapshotName: "checkpoint", NodeVmsWithLocalSnapshots: []string{worker.GetNodeName()}}
					reason = store.OwnershipLocalCheckpoint
					if scenario == "upload" {
						a.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDING
						a.Status.InProgressSnapshotUri = "gs://pending/upload"
						reason = store.OwnershipOperationPending
					}
				}
				if scenario == "actor-pointer" {
					a.Status.State = ateapipb.ActorState_ACTOR_STATE_RESUMING
					a.Status.WorkerAssignment = &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, WorkerNamespace: worker.GetWorkerNamespace(), WorkerPod: worker.GetWorkerPod(), WorkerPodUid: worker.GetWorkerPodUid(), NodeName: worker.GetNodeName()}
					reason = store.OwnershipBusy
				}
				if scenario == "unknown-actor" {
					a.Status.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
					reason = store.OwnershipUnknown
				}
				created, err := p.CreateActor(ctx, a)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "corrupt-actor" {
					if _, err := p.pool.Exec(ctx, `UPDATE actors SET proto = $1 WHERE uid = $2`, []byte{0xff}, created.GetMetadata().GetUid()); err != nil {
						t.Fatal(err)
					}
					reason = store.OwnershipUnknown
				}
			}
			snapshot := func() ([]byte, int64, int64, int64) {
				t.Helper()
				var data []byte
				var version, outbox, assignments int64
				if err := p.pool.QueryRow(ctx, `SELECT proto, version FROM workers WHERE name = $1`, worker.GetMetadata().GetName()).Scan(&data, &version); err != nil {
					t.Fatal(err)
				}
				if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM worker_outbox`).Scan(&outbox); err != nil {
					t.Fatal(err)
				}
				if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM worker_assignments`).Scan(&assignments); err != nil {
					t.Fatal(err)
				}
				return data, version, outbox, assignments
			}
			before, version, outbox, assignments := snapshot()
			var actorBefore []byte
			_ = p.pool.QueryRow(ctx, `SELECT proto FROM actors LIMIT 1`).Scan(&actorBefore)
			proof, err := p.ProveEmptyDraining(ctx, expected)
			requireOwnershipRefusal(t, proof, err, reason)
			after, versionAfter, outboxAfter, assignmentsAfter := snapshot()
			var actorAfter []byte
			_ = p.pool.QueryRow(ctx, `SELECT proto FROM actors LIMIT 1`).Scan(&actorAfter)
			if !bytes.Equal(before, after) || version != versionAfter || outbox != outboxAfter || assignments != assignmentsAfter || !bytes.Equal(actorBefore, actorAfter) {
				t.Fatal("refusal mutated durable state")
			}
		})
	}
}

func TestProveEmptyDraining_IdentityAndNameReuse(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := context.Background()
	worker, err := p.CreateWorker(ctx, ownershipTestWorker())
	if err != nil {
		t.Fatal(err)
	}
	drained, err := p.DrainEmptyWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(worker))
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range []func(*ateapipb.Worker){func(w *ateapipb.Worker) { w.Metadata.Uid = "" }, func(w *ateapipb.Worker) { w.Metadata.Version = 0 }, func(w *ateapipb.Worker) { w.Metadata.Version = -1 }} {
		expected := proto.CloneOf(drained)
		missing(expected)
		if proof, err := p.ProveEmptyDraining(ctx, expected); proof != nil || !errors.Is(err, store.ErrPreconditionRequired) {
			t.Fatalf("missing guard: %v %v", proof, err)
		}
	}
	if proof, err := p.ProveEmptyDraining(ctx, worker); proof != nil || !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("stale: %v %v", proof, err)
	}
	if _, err := p.DeleteWorker(ctx, drained.GetMetadata().GetName(), store.DeletePreconditions{UID: drained.GetMetadata().GetUid()}); err != nil {
		t.Fatal(err)
	}
	if proof, err := p.ProveEmptyDraining(ctx, drained); proof != nil || !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: %v %v", proof, err)
	}
	if _, err := p.CreateWorker(ctx, ownershipTestWorker()); err != nil {
		t.Fatal(err)
	}
	if proof, err := p.ProveEmptyDraining(ctx, drained); proof != nil || !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("name reuse: %v %v", proof, err)
	}
}

func TestProveEmptyDraining_PublicationAndRetentionInterleaving(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	createTestAtespace(t, p, "proof-team")
	worker, err := p.CreateWorker(ctx, ownershipTestWorker())
	if err != nil {
		t.Fatal(err)
	}
	worker, err = p.DrainEmptyWorker(ctx, worker.GetMetadata().GetName(), store.PreconditionFrom(worker))
	if err != nil {
		t.Fatal(err)
	}
	actor, err := p.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "proof-team", Name: "publishing"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}})
	if err != nil {
		t.Fatal(err)
	}
	ref := resources.ActorRefFromActor(actor)
	entered, publish := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := p.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
			close(entered)
			select {
			case <-publish:
			case <-ctx.Done():
				return ctx.Err()
			}
			a.Status.State = ateapipb.ActorState_ACTOR_STATE_PAUSED
			a.Status.LocalSnapshot = &ateapipb.LocalSnapshot{SnapshotName: "checkpoint", NodeVmsWithLocalSnapshots: []string{worker.GetNodeName()}}
			return nil
		})
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	proof, proofErr := p.ProveEmptyDraining(ctx, worker)
	close(publish)
	updateErr := <-result
	requireOwnershipRefusal(t, proof, proofErr, store.OwnershipStabilityUnproven)
	if updateErr != nil {
		t.Fatal(updateErr)
	}
	proof, proofErr = p.ProveEmptyDraining(ctx, worker)
	requireOwnershipRefusal(t, proof, proofErr, store.OwnershipLocalCheckpoint)
	actor, err = p.GetActor(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	// This is the same atomic retention-publication store boundary used by
	// FinalizeSuspended, not an upload simulation or proof that bytes exist.
	if _, err := p.UpdateActor(ctx, ref, store.PreconditionFrom(actor), func(a *ateapipb.Actor) error {
		a.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
		a.Status.LocalSnapshot = nil
		a.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{SnapshotUri: "gs://independent/retained"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	proof, proofErr = p.ProveEmptyDraining(ctx, worker)
	requireOwnershipRefusal(t, proof, proofErr, store.OwnershipStabilityUnproven)
}

// These unit cases exercise production dependency policy without a database;
// nil means only no recorded local dependency, never release eligibility.
func TestActorRetirementDependency(t *testing.T) {
	worker := ownershipTestWorker()
	for _, tc := range []struct {
		name   string
		status *ateapipb.ActorStatus
		reason string
	}{
		{"external-independent", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ExternalSnapshot: &ateapipb.ExternalSnapshot{SnapshotUri: "gs://retained/object"}}, ""},
		{"local-on-node", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "checkpoint", NodeVmsWithLocalSnapshots: []string{"node"}}}, store.OwnershipLocalCheckpoint},
		{"local-on-other-node", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "checkpoint", NodeVmsWithLocalSnapshots: []string{"unrelated-node"}}}, ""},
		{"upload-on-node", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, InProgressSnapshotUri: "gs://pending/upload", LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "checkpoint", NodeVmsWithLocalSnapshots: []string{"node"}}}, store.OwnershipOperationPending},
		{"unlocated-upload", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, InProgressSnapshotUri: "gs://pending/upload"}, store.OwnershipUnknown},
		{"missing-checkpoint-node", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED, LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "checkpoint"}}, store.OwnershipUnknown},
		{"paused-without-checkpoint", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSED}, store.OwnershipUnknown},
		{"missing-status", nil, store.OwnershipUnknown},
		{"unknown-state", &ateapipb.ActorStatus{State: ateapipb.ActorState(100)}, store.OwnershipUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := actorRetirementDependency(&ateapipb.Actor{Status: tc.status}, worker)
			if tc.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			requireOwnershipRefusal(t, nil, err, tc.reason)
		})
	}
}

func TestOwnershipUnknown_NestedResources(t *testing.T) {
	worker := ownershipTestWorker()
	worker.Status.Allocated = &ateapipb.WorkerResources{Resources: &ateapipb.Resources{}}
	if ownershipUnknown(worker.ProtoReflect()) {
		t.Fatal("known worker reported unknown")
	}
	worker.Status.Allocated.Resources.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	data, err := proto.Marshal(worker)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &ateapipb.Worker{}
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	if !ownershipUnknown(decoded.ProtoReflect()) {
		t.Fatal("nested unknown ownership data accepted")
	}
}
