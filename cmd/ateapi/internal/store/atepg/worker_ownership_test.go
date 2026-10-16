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
	"github.com/jackc/pgx/v5"
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
	for _, scenario := range []string{"empty", "active", "mapping", "local", "upload", "external", "unknown-worker", "unknown-actor", "corrupt-actor", "busy", "allocated", "actor-pointer", "running-missing-pointer", "unknown-metadata"} {
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
			reason := ""
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
			if scenario == "local" || scenario == "upload" || scenario == "external" || scenario == "unknown-actor" || scenario == "corrupt-actor" || scenario == "actor-pointer" || scenario == "running-missing-pointer" {
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
				if scenario == "running-missing-pointer" {
					a.Status.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
					a.Status.LocalSnapshot = &ateapipb.LocalSnapshot{SnapshotName: "checkpoint", NodeVmsWithLocalSnapshots: []string{worker.GetNodeName()}}
					reason = store.OwnershipUnknown
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
			if err := p.pool.QueryRow(ctx, `SELECT proto FROM actors LIMIT 1`).Scan(&actorBefore); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal(err)
			}
			proof, err := p.ProveEmptyDraining(ctx, expected)
			if reason == "" {
				requireOwnershipProof(t, proof, err, expected)
			} else {
				requireOwnershipRefusal(t, proof, err, reason)
			}
			after, versionAfter, outboxAfter, assignmentsAfter := snapshot()
			var actorAfter []byte
			if err := p.pool.QueryRow(ctx, `SELECT proto FROM actors LIMIT 1`).Scan(&actorAfter); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal(err)
			}
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
	// A new store instance recovers a lost proof response from durable records.
	fresh, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.pool.Close()
	defer fresh.Close()
	proof, proofErr := fresh.ProveEmptyDraining(ctx, drained)
	requireOwnershipProof(t, proof, proofErr, drained)
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
	actor, err := p.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "proof-team", Name: "publishing"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_PAUSING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: worker.GetMetadata().GetName()}, WorkerNamespace: worker.GetWorkerNamespace(), WorkerPod: worker.GetWorkerPod(), WorkerPodUid: worker.GetWorkerPodUid(), NodeName: worker.GetNodeName()}}})
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
			a.Status.WorkerAssignment = nil
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
	requireOwnershipRefusal(t, proof, proofErr, store.OwnershipBusy)
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
	requireOwnershipProof(t, proof, proofErr, worker)
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
		{"running-unqualified-pointer", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "unrelated-worker"}, NodeName: "unrelated-node"}, LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "historical", NodeVmsWithLocalSnapshots: []string{"node"}}}, store.OwnershipUnknown},
		{"running-missing-pointer", &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING, LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "historical", NodeVmsWithLocalSnapshots: []string{"node"}}}, store.OwnershipUnknown},
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
			err := actorRetirementDependency(&ateapipb.Actor{Status: tc.status}, worker, nil, nil)
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

// This policy test supplies a complete live claim observation. Production
// obtains it from the joined SQL read; the lifecycle test constructs it through
// Pause and Resume rather than treating a hand-written status as migration.
func TestQualifiedRunningAssignment(t *testing.T) {
	live := ownershipTestWorker()
	live.Metadata.Uid = "22222222-2222-4222-8222-222222222222"
	live.Metadata.Version = 3
	live.Status.Allocated = &ateapipb.WorkerResources{Actors: 1}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: "actor", Uid: "33333333-3333-4333-8333-333333333333"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: live.GetMetadata().GetName()},
				WorkerNamespace: live.GetWorkerNamespace(),
				WorkerPool:      live.GetWorkerPool(),
				WorkerPod:       live.GetWorkerPod(),
				WorkerPodUid:    live.GetWorkerPodUid(),
				WorkerPodIp:     live.GetIp(),
				NodeName:        live.GetNodeName(),
			},
			LocalSnapshot: &ateapipb.LocalSnapshot{SnapshotName: "retained", NodeVmsWithLocalSnapshots: []string{live.GetNodeName()}},
		},
	}
	claim := &ateapipb.ActorAssignment{Actor: &ateapipb.ObjectRef{Atespace: "team", Name: "actor"}, ActorUid: actor.GetMetadata().GetUid()}
	for _, tc := range []struct {
		name   string
		mutate func(*ateapipb.Actor, *ateapipb.Worker, *ateapipb.ActorAssignment)
	}{
		{"missing-pointer", func(a *ateapipb.Actor, _ *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			a.Status.WorkerAssignment = nil
		}},
		{"missing-pod-uid", func(a *ateapipb.Actor, _ *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			a.Status.WorkerAssignment.WorkerPodUid = ""
		}},
		{"missing-pool", func(a *ateapipb.Actor, _ *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			a.Status.WorkerAssignment.WorkerPool = ""
		}},
		{"missing-ip", func(a *ateapipb.Actor, _ *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			a.Status.WorkerAssignment.WorkerPodIp = ""
		}},
		{"unrelated-snapshot-node", func(a *ateapipb.Actor, _ *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			a.Status.LocalSnapshot.NodeVmsWithLocalSnapshots = []string{"unrelated"}
		}},
		{"operation-pending", func(a *ateapipb.Actor, _ *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			a.Status.InProgressLocalSnapshotName = "pending"
		}},
		{"worker-identity-reused", func(_ *ateapipb.Actor, w *ateapipb.Worker, _ *ateapipb.ActorAssignment) {
			w.WorkerPodUid = "44444444-4444-4444-8444-444444444444"
		}},
		{"empty-allocation", func(_ *ateapipb.Actor, w *ateapipb.Worker, _ *ateapipb.ActorAssignment) { w.Status.Allocated = nil }},
		{"actor-identity-reused", func(_ *ateapipb.Actor, _ *ateapipb.Worker, c *ateapipb.ActorAssignment) {
			c.ActorUid = "44444444-4444-4444-8444-444444444444"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, w, c := proto.CloneOf(actor), proto.CloneOf(live), proto.CloneOf(claim)
			tc.mutate(a, w, c)
			if qualifiedRunningAssignment(a, w, c) {
				t.Fatal("malformed ownership qualified as live")
			}
		})
	}
	if !qualifiedRunningAssignment(actor, live, claim) {
		t.Fatal("complete live claim did not qualify")
	}
	if qualifiedRunningAssignment(actor, nil, claim) || qualifiedRunningAssignment(actor, live, nil) {
		t.Fatal("missing live worker or durable claim qualified")
	}
	// Even qualified historical metadata leaves the current node dependency.
	err := actorRetirementDependency(actor, live, live, claim)
	requireOwnershipRefusal(t, nil, err, store.OwnershipBusy)
	other := proto.CloneOf(live)
	other.Metadata.Name = "other-worker"
	other.NodeName = "other-node"
	if err := actorRetirementDependency(actor, other, live, claim); err != nil {
		t.Fatalf("independent node acquired a dependency on historical metadata: %v", err)
	}
}

func requireOwnershipProof(t *testing.T, proof *ateapipb.ProveEmptyDrainingResponse, err error, worker *ateapipb.Worker) {
	t.Helper()
	if err != nil || proof == nil || !proto.Equal(proof.GetWorker(), worker) || len(proof.GetOwnershipGeneration()) != len("observed-v1:")+64 {
		t.Fatalf("incomplete exact worker proof: proof=%v err=%v", proof, err)
	}
}
