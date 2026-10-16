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
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Fault only the origin write before dispatch. All successful writes, claims,
// leases, admission and lifecycle transitions use the real PostgreSQL store.
type beforeDispatchStore struct {
	store.Interface
	mu          sync.Mutex
	fail        bool
	failRunning bool
}

func (s *beforeDispatchStore) UpdateActor(ctx context.Context, ref resources.ActorRef, pre store.Precondition, update func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	return s.Interface.UpdateActor(ctx, ref, pre, func(a *ateapipb.Actor) error {
		if err := update(a); err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failRunning && a.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			s.failRunning = false
			return status.Error(codes.Unavailable, "injected running publication failure")
		}
		if s.fail && a.GetStatus().GetPendingNativeOperation().GetOperation() == "run" {
			s.fail = false
			return status.Error(codes.Unavailable, "injected pre-dispatch persistence failure")
		}
		return nil
	})
}

func protectLifecyclePod(t *testing.T, tc *testContext, ctx context.Context, ns, name string, protect bool) {
	t.Helper()
	pod, err := tc.k8sClient.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if protect {
		pod.Annotations = map[string]string{"karpenter.sh/do-not-disrupt": "true", "controller.kubernetes.io/pod-deletion-cost": "1000"}
		pod.Labels["goat.tregoat.eu/actor-active"] = "true"
	} else {
		delete(pod.Annotations, "karpenter.sh/do-not-disrupt")
	}
	if _, err := tc.k8sClient.CoreV1().Pods(ns).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// Exercises the real RPC/workflow/gate/PG composition with enforcement enabled:
// migration, pre-dispatch interruption, reservation-preserving retry, successful
// resume, local pause, restore, external upload, and independent sibling work.
func TestWorkerProtection_ProductionLifecycle(t *testing.T) {
	ns := namespaceForTest("ns-protected-lifecycle")
	fault := &beforeDispatchStore{fail: true}
	tc := setupTestWithWrappedStore(t, ns, nil, func(p store.Interface) store.Interface { fault.Interface = p; return fault }, func(s *controlapi.RPCService) {
		kube, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.EnableWorkerProtection(kube)
	})
	defer tc.cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	createTemplate(t, tc, ns)
	workerName := createWorkerPod(t, tc, ns, "old-worker", "node1", "pool1")
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "protected"}
	created, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: ref.Name}, ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err == nil {
		t.Fatal("ACTIVE unprotected worker admitted")
	}
	unchanged, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil || !proto.Equal(unchanged.GetStatus(), created.GetStatus()) {
		t.Fatalf("refusal changed actor: %v %v", unchanged, err)
	}
	protectLifecyclePod(t, tc, ctx, ns, "old-worker", true)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err == nil {
		t.Fatal("pre-dispatch fault did not interrupt resume")
	}
	assigned, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil || assigned.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING || assigned.GetStatus().GetWorkerAssignment() == nil || assigned.GetStatus().GetPendingNativeOperation() != nil {
		t.Fatalf("missing retryable reservation: %v %v", assigned, err)
	}
	worker, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}})
	if err != nil {
		t.Fatal(err)
	}
	protectLifecyclePod(t, tc, ctx, ns, "old-worker", false)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err == nil {
		t.Fatal("unprotected retry admitted")
	}
	after, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil || !proto.Equal(after, assigned) {
		t.Fatalf("retry lost actor reservation: %v %v", after, err)
	}
	if !proto.Equal(retirementReadback(t, tc, ctx, worker), worker) {
		t.Fatal("retry changed worker allocation")
	}
	if _, err := tc.persistence.GetWorkerAssignment(ctx, workerName, assigned.GetMetadata().GetUid()); err != nil {
		t.Fatalf("retry lost durable claim: %v", err)
	}
	tc.fakeAtelet.Lock.Lock()
	called := tc.fakeAtelet.RunCalled
	tc.fakeAtelet.Lock.Unlock()
	if called {
		t.Fatal("refused/pre-dispatch resume reached native")
	}
	protectLifecyclePod(t, tc, ctx, ns, "old-worker", true)
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	running, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil || running.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || running.GetStatus().GetLocalSnapshot() == nil || running.GetStatus().GetPendingNativeOperation() != nil {
		t.Fatalf("pause/resume did not settle: %v %v", running, err)
	}
	if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatal(err)
	}
	worker = retirementReadback(t, tc, ctx, worker)
	drained, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()})
	if err != nil {
		t.Fatal(err)
	}
	siblingName := createWorkerPod(t, tc, ns, "sibling", "node1", "pool1")
	protectLifecyclePod(t, tc, ctx, ns, "sibling", true)
	_, chosen := retirementActor(t, tc, ctx)
	if chosen.GetMetadata().GetName() != siblingName {
		t.Fatal("protected independent sibling not chosen")
	}
	proof, err := tc.client.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained})
	if err != nil || proof.GetOwnershipGeneration() == "" {
		t.Fatalf("external-only history blocked empty worker: %v %v", proof, err)
	}
}

// Native Run returned success, but publication failed. A reconstructed workflow
// must retain the durable origin rather than infer completion or replay Run.
func TestWorkerProtection_ResumePublicationFailureDoesNotReplay(t *testing.T) {
	ns := namespaceForTest("ns-protected-no-replay")
	fault := &beforeDispatchStore{failRunning: true}
	tc := setupTestWithWrappedStore(t, ns, nil, func(p store.Interface) store.Interface { fault.Interface = p; return fault }, func(s *controlapi.RPCService) {
		kube, err := kubernetes.NewForConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		s.EnableWorkerProtection(kube)
	})
	defer tc.cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	createTemplate(t, tc, ns)
	workerName := createWorkerPod(t, tc, ns, "worker", "node1", "pool1")
	protectLifecyclePod(t, tc, ctx, ns, "worker", true)
	ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "interrupted"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: ref.Name}, ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err == nil {
		t.Fatal("publication fault not observed")
	}
	pending, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil || pending.GetStatus().GetPendingNativeOperation().GetOperation() != "run" || pending.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("lost durable run origin: %v %v", pending, err)
	}
	tc.fakeAtelet.Lock.Lock()
	called := tc.fakeAtelet.RunCalled
	tc.fakeAtelet.RunCalled = false
	tc.fakeAtelet.Lock.Unlock()
	if !called {
		t.Fatal("fault occurred before native success")
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("terminal receipt did not recover publication: %v", err)
	}
	tc.fakeAtelet.Lock.Lock()
	replayed := tc.fakeAtelet.RunCalled
	tc.fakeAtelet.Lock.Unlock()
	if replayed {
		t.Fatal("native Run replayed without a receipt")
	}
	after, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil || after.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || after.GetStatus().GetPendingNativeOperation() != nil {
		t.Fatalf("receipt publication did not settle: %v %v", after, err)
	}
	worker, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unsettled run allowed drain: %v", err)
	}
}

// Real PG leases, actor CAS, worker admission and lifecycle publication with
// only the native/Kubernetes external boundaries faked. Rebuild the production
// service before reconciliation; no in-memory producer ACK survives.
func TestWorkerProtection_TerminalReceiptRecovery(t *testing.T) {
	for _, operation := range []string{"run", "restore", "checkpoint-local", "checkpoint-external", "upload-local", "terminate"} {
		t.Run(operation, func(t *testing.T) {
			ns := namespaceForTest("ns-receipt-" + operation)
			tc := setupTestWithWrappedStore(t, ns, nil, nil, func(s *controlapi.RPCService) {
				kube, err := kubernetes.NewForConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				s.EnableWorkerProtection(kube)
			})
			defer tc.cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			createTemplate(t, tc, ns)
			workerName := createWorkerPod(t, tc, ns, "worker", "node1", "pool1")
			protectLifecyclePod(t, tc, ctx, ns, "worker", true)
			ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "recover"}
			if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: ref.Name}, ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"}}}); err != nil {
				t.Fatal(err)
			}
			if operation != "run" {
				if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
			}
			if operation == "restore" || operation == "upload-local" {
				if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
			}
			action := func(s *controlapi.RPCService) error {
				switch operation {
				case "run", "restore":
					_, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
					return err
				case "checkpoint-local":
					_, err := s.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref})
					return err
				case "checkpoint-external", "upload-local":
					_, err := s.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
					return err
				case "terminate":
					_, err := s.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true})
					return err
				}
				return nil
			}
			tc.fakeAtelet.Lock.Lock()
			before := tc.fakeAtelet.OperationExecutions
			tc.fakeAtelet.LoseOperationACK = true
			tc.fakeAtelet.FailOperationLookup = true
			tc.fakeAtelet.Lock.Unlock()
			if err := action(tc.service); err == nil {
				t.Fatal("lost ACK and lookup outage reported success")
			}
			pending, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err != nil || pending.GetStatus().GetPendingNativeOperation().GetOperation() != operation || len(pending.GetStatus().GetPendingNativeOperation().GetInvocation()) == 0 {
				t.Fatalf("exact invocation lost: %v %v", pending, err)
			}
			worker, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}})
			if err != nil {
				t.Fatal(err)
			}
			drained, drainErr := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()})
			if operation == "upload-local" {
				if drainErr != nil {
					t.Fatal(drainErr)
				}
				if _, err := tc.client.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("unpublished upload allowed proof: %v", err)
				}
			} else if status.Code(drainErr) != codes.FailedPrecondition {
				t.Fatalf("owned assignment allowed drain: %v", drainErr)
			}
			restarted := tc.rebuildService()
			if err := action(restarted); err == nil {
				t.Fatal("lookup outage invented completion")
			}
			tc.fakeAtelet.Lock.Lock()
			tc.fakeAtelet.FailOperationLookup = false
			tc.fakeAtelet.Lock.Unlock()
			if err := action(restarted); err != nil {
				t.Fatalf("terminal receipt did not recover %s: %v", operation, err)
			}
			tc.fakeAtelet.Lock.Lock()
			executions := tc.fakeAtelet.OperationExecutions
			tc.fakeAtelet.Lock.Unlock()
			if executions != before+1 {
				t.Fatalf("native replayed: executions %d before %d", executions, before)
			}
			after, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if operation == "terminate" {
				if status.Code(err) != codes.NotFound {
					t.Fatalf("delete not settled: %v", err)
				}
				return
			}
			if err != nil || after.GetStatus().GetPendingNativeOperation() != nil {
				t.Fatalf("publication did not settle: %v %v", after, err)
			}
			expected := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
			if operation == "run" || operation == "restore" {
				expected = ateapipb.ActorState_ACTOR_STATE_RUNNING
			}
			if operation == "checkpoint-local" {
				expected = ateapipb.ActorState_ACTOR_STATE_PAUSED
			}
			if after.GetStatus().GetState() != expected {
				t.Fatalf("wrong publication: %v", after)
			}
		})
	}
}

func TestWorkerProtection_OperationDrainInterleavings(t *testing.T) {
	for _, op := range []string{"run", "checkpoint", "upload"} {
		t.Run(op, func(t *testing.T) {
			ns := namespaceForTest("ns-gated-barrier-" + op)
			tc := setupTestWithWrappedStore(t, ns, nil, nil, func(s *controlapi.RPCService) {
				kube, err := kubernetes.NewForConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				s.EnableWorkerProtection(kube)
			})
			defer tc.cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			createTemplate(t, tc, ns)
			workerName := createWorkerPod(t, tc, ns, "worker", "node1", "pool1")
			protectLifecyclePod(t, tc, ctx, ns, "worker", true)
			ref := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "barrier"}
			if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: ref.Name}, ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"}}}); err != nil {
				t.Fatal(err)
			}
			if op != "run" {
				if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
			}
			if op == "upload" {
				if _, err := tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			barrier := func(ctx context.Context) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			tc.fakeAtelet.Lock.Lock()
			switch op {
			case "run":
				tc.fakeAtelet.BeforeRun = func(ctx context.Context, _ *ateletpb.RunRequest) error { return barrier(ctx) }
			case "checkpoint":
				tc.fakeAtelet.BeforeCheckpoint = func(ctx context.Context, _ *ateletpb.CheckpointRequest) error { return barrier(ctx) }
			case "upload":
				tc.fakeAtelet.BeforeUpload = func(ctx context.Context, _ *ateletpb.UploadPausedCheckpointRequest) error { return barrier(ctx) }
			}
			tc.fakeAtelet.Lock.Unlock()
			done := make(chan error, 1)
			go func() {
				var err error
				switch op {
				case "run":
					_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
				case "checkpoint":
					_, err = tc.client.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: ref})
				case "upload":
					_, err = tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
				}
				done <- err
			}()
			defer func() { once.Do(func() { close(release) }); <-done }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			worker, err := tc.client.GetWorker(ctx, &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}})
			if err != nil {
				t.Fatal(err)
			}
			drained, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: workerName}, Uid: worker.GetMetadata().GetUid(), Version: worker.GetMetadata().GetVersion()})
			if op == "upload" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tc.client.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("inflight upload allowed proof: %v", err)
				}
			} else if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("inflight assignment allowed drain: %v", err)
			}
			once.Do(func() { close(release) })
			// Put the result back for the cleanup join, so failures also leave no worker.
			result := <-done
			done <- result
			if result != nil {
				t.Fatal(result)
			}
			if op == "upload" {
				if _, err := tc.client.ProveEmptyDraining(ctx, &ateapipb.ProveEmptyDrainingRequest{ExpectedWorker: drained}); err != nil {
					t.Fatalf("settled upload did not allow proof: %v", err)
				}
			}
		})
	}
}
