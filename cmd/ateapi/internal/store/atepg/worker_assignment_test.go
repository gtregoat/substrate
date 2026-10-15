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
	"context"
	"errors"
	"fmt"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestSaveWorker_RejectsAStaleWrite proves the precondition saveWorker states
// on top of the row lock its callers hold: a Worker read before someone else
// wrote it cannot overwrite that write.
func TestSaveWorker_RejectsAStaleWrite(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	p, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	clearAll(t, p)

	created, err := p.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "stale-write-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}

	// Move the stored Worker on, so the copy above is a version behind.
	if _, err := p.UpdateWorker(ctx, created.GetMetadata().GetName(), store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
		toUpdate.Ip = "10.0.0.1"
		return nil
	}); err != nil {
		t.Fatalf("UpdateWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("saveWorker() with a stale Worker = %v, want ErrVersionConflict", err)
	}
}

// TestSaveWorker_RejectsAVanishedWorker keeps a deleted row from being an
// update of nothing.
func TestSaveWorker_RejectsAVanishedWorker(t *testing.T) {
	requirePool(t)
	ctx := context.Background()

	p, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.pool.Close()
	defer p.Close()
	clearAll(t, p)

	created, err := p.CreateWorker(ctx, &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: "vanished-worker"},
		WorkerNamespace: "ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod",
	})
	if err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if _, err := p.DeleteWorker(ctx, created.GetMetadata().GetName(), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteWorker failed: %v", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)
	if err := saveWorker(ctx, tx, created); !errors.Is(err, store.ErrVersionConflict) {
		t.Errorf("saveWorker() on a deleted Worker = %v, want ErrVersionConflict", err)
	}
}

// Both orders use the production row-lock paths. The first transaction's
// mutation/admission callback is a barrier while it owns the worker lock.
func TestDrainEmptyWorker_BindingOrders(t *testing.T) {
	for _, assignmentFirst := range []bool{true, false} {
		t.Run(fmt.Sprint("assignment-first-", assignmentFirst), func(t *testing.T) {
			p := setupPostgresPersistence(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			worker, err := p.CreateWorker(ctx, &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "empty-drain"}, Status: &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Capacity: &ateapipb.WorkerResources{Actors: 2}}})
			if err != nil {
				t.Fatal(err)
			}
			scheduler := scheduling.New(nil)
			admit := func(w *ateapipb.Worker) error {
				if !scheduler.Applies(w, scheduling.Constraints{}) || !scheduler.HasRoom(w, scheduling.Constraints{}) {
					return store.ErrFailedPrecondition
				}
				return nil
			}
			assignment := &ateapipb.ActorAssignment{ActorUid: "actor-in-resume"}
			var resuming *ateapipb.Actor
			if assignmentFirst {
				createTestAtespace(t, p, "ordering-team")
				resuming, err = p.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ordering-team", Name: "resuming"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "empty-drain"}}}})
				if err != nil {
					t.Fatal(err)
				}
				assignment.ActorUid = resuming.GetMetadata().GetUid()
			}
			locked, release := make(chan struct{}), make(chan struct{})
			first := make(chan error, 1)
			second := make(chan error, 1)
			if assignmentFirst {
				go func() {
					first <- p.BindActorToWorker(ctx, "empty-drain", assignment, func(w *ateapipb.Worker) error {
						close(locked)
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
						return admit(w)
					})
				}()
				select {
				case <-locked:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				go func() {
					_, err := p.DrainEmptyWorker(ctx, "empty-drain", store.PreconditionFrom(worker))
					second <- err
				}()
			} else {
				// Hold the production row lock until PostgreSQL confirms the drain
				// is queued. PostgreSQL grants conflicting row locks in queue order,
				// so the later bind waits behind the production drain transaction.
				tx, err := p.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(context.Background())
				if _, err := getWorkerForUpdate(ctx, tx, "empty-drain"); err != nil {
					t.Fatal(err)
				}
				go func() { _, err := p.DrainEmptyWorker(ctx, "empty-drain", store.PreconditionFrom(worker)); first <- err }()
				waitForWorkerLockWaiter(t, ctx, p)
				go func() { second <- p.BindActorToWorker(ctx, "empty-drain", assignment, admit) }()
				if err := tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if assignmentFirst {
				waitForWorkerLockWaiter(t, ctx, p)
				close(release)
			}
			if err := <-first; err != nil {
				t.Fatalf("first transaction: %v", err)
			}
			err = <-second
			if assignmentFirst {
				if !errors.Is(err, store.ErrVersionConflict) {
					t.Fatalf("drain after bind: %v", err)
				}
			} else if !errors.Is(err, store.ErrFailedPrecondition) {
				t.Fatalf("bind after drain: %v", err)
			}
			got, err := p.GetWorker(ctx, "empty-drain")
			if err != nil {
				t.Fatal(err)
			}
			want := ateapipb.WorkerState_WORKER_STATE_DRAINING
			if assignmentFirst {
				want = ateapipb.WorkerState_WORKER_STATE_ACTIVE
			}
			if got.GetStatus().GetState() != want {
				t.Fatalf("state = %v", got.GetStatus().GetState())
			}
			bound, err := p.GetWorkerAssignment(ctx, "empty-drain", assignment.GetActorUid())
			if assignmentFirst {
				actorAfter, actorErr := p.GetActor(ctx, resources.ActorRefFromActor(resuming))
				if actorErr != nil || !proto.Equal(resuming, actorAfter) {
					t.Fatalf("RESUMING actor changed: %v %v", actorAfter, actorErr)
				}
				if err != nil || bound.GetActorUid() != assignment.GetActorUid() || got.GetStatus().GetAllocated().GetActors() != 1 {
					t.Fatalf("binding changed: %v, %v", bound, err)
				}
			} else if !errors.Is(err, store.ErrNotFound) || got.GetStatus().GetAllocated() != nil {
				t.Fatalf("drained worker bound: %v, %v", bound, err)
			}
		})
	}
}

func waitForWorkerLockWaiter(t *testing.T, ctx context.Context, p *Persistence) {
	t.Helper()
	for {
		var waiting bool
		err := p.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE 'SELECT proto FROM workers%FOR UPDATE%')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func TestDrainEmptyWorker_GuardsAndRecovery(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := context.Background()
	worker, err := p.CreateWorker(ctx, &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "drain-guards"}, Status: &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}})
	if err != nil {
		t.Fatal(err)
	}
	for _, guard := range []store.Precondition{{}, {UID: worker.GetMetadata().GetUid()}, {Version: 1}, {UID: "other", Version: 1}, {UID: worker.GetMetadata().GetUid(), Version: 99}} {
		if _, err := p.DrainEmptyWorker(ctx, "drain-guards", guard); err == nil {
			t.Fatalf("accepted guard: %+v", guard)
		}
		got, err := p.GetWorker(ctx, "drain-guards")
		if err != nil || !proto.Equal(worker, got) {
			t.Fatalf("guard wrote worker: %v, %v", got, err)
		}
	}
	guard := store.PreconditionFrom(worker)
	results := make(chan error, 2)
	for range 2 {
		go func() { _, err := p.DrainEmptyWorker(ctx, "drain-guards", guard); results <- err }()
	}
	successes := 0
	for range 2 {
		if err := <-results; err == nil {
			successes++
		} else if !errors.Is(err, store.ErrVersionConflict) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("successes = %d", successes)
	}
	// Discard the acknowledgement and reconnect, as a restarted consumer would.
	fresh, err := Connect(ctx, containerDSN, "public")
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.pool.Close()
	defer fresh.Close()
	readback, err := fresh.GetWorker(ctx, "drain-guards")
	if err != nil || readback.GetMetadata().GetUid() != guard.UID || readback.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Fatalf("readback: %v %v", readback, err)
	}
	if _, err := fresh.DrainEmptyWorker(ctx, "drain-guards", guard); !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("old retry: %v", err)
	}
	reconciled, err := fresh.DrainEmptyWorker(ctx, "drain-guards", store.PreconditionFrom(readback))
	if err != nil || !proto.Equal(readback, reconciled) {
		t.Fatalf("reconcile: %v %v", reconciled, err)
	}
	if _, err := p.DeleteWorker(ctx, "drain-guards", store.DeletePreconditions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CreateWorker(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DrainEmptyWorker(ctx, "drain-guards", store.PreconditionFrom(readback)); !errors.Is(err, store.ErrUIDConflict) {
		t.Fatalf("name reuse: %v", err)
	}
}

func TestDrainEmptyWorker_BusyResumingActor(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, p, "drain-team")
	worker, err := p.CreateWorker(ctx, &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "busy-drain"}, Status: &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}})
	if err != nil {
		t.Fatal(err)
	}
	actor, err := p.CreateActor(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "drain-team", Name: "resuming"}, Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING, WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: "busy-drain"}}}})
	if err != nil {
		t.Fatal(err)
	}
	// A pointer alone must refuse even if allocation and assignment are absent.
	if _, err := p.DrainEmptyWorker(ctx, "busy-drain", store.PreconditionFrom(worker)); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("actor pointer: %v", err)
	}
	if err := p.BindActorToWorker(ctx, "busy-drain", &ateapipb.ActorAssignment{ActorUid: actor.GetMetadata().GetUid()}, nil); err != nil {
		t.Fatal(err)
	}
	busy, err := p.GetWorker(ctx, "busy-drain")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.DrainEmptyWorker(ctx, "busy-drain", store.PreconditionFrom(busy)); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("busy: %v", err)
	}
	after, err := p.GetWorker(ctx, "busy-drain")
	if err != nil || !proto.Equal(after, busy) {
		t.Fatalf("worker changed: %v %v", after, err)
	}
	actorAfter, err := p.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil || !proto.Equal(actorAfter, actor) {
		t.Fatalf("RESUMING actor changed: %v %v", actorAfter, err)
	}
	assignment, err := p.GetWorkerAssignment(ctx, "busy-drain", actor.GetMetadata().GetUid())
	if err != nil || assignment.GetActorUid() != actor.GetMetadata().GetUid() {
		t.Fatalf("assignment changed: %v %v", assignment, err)
	}
}

func TestDrainEmptyWorker_AllocationAndUnknown(t *testing.T) {
	for _, allocated := range []*ateapipb.WorkerResources{
		{Actors: 1},
		{Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{{Name: "cpu", Quantity: "1"}}}},
	} {
		p := setupPostgresPersistence(t)
		ctx := context.Background()
		worker, err := p.CreateWorker(ctx, &ateapipb.Worker{Metadata: &ateapipb.ResourceMetadata{Name: "allocated-drain"}, Status: &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE, Allocated: allocated}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.DrainEmptyWorker(ctx, "allocated-drain", store.PreconditionFrom(worker)); !errors.Is(err, store.ErrFailedPrecondition) {
			t.Fatalf("allocation-only: %v", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := p.GetWorker(canceled, "allocated-drain"); err == nil {
			t.Fatal("unavailable read was treated as known")
		}
		if _, err := p.DrainEmptyWorker(canceled, "allocated-drain", store.PreconditionFrom(worker)); err == nil {
			t.Fatal("unavailable drain was treated as success")
		}
		if _, err := p.GetWorker(ctx, "absent-drain"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("missing read: %v", err)
		}
	}
}
