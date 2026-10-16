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
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Faults surround real PG settlement transactions; lifecycle, binding and
// resource cleanup remain production implementations.
type terminationSettlementStore struct {
	store.Interface
	mu       sync.Mutex
	mode     string
	failRead bool
}

func (s *terminationSettlementStore) GetActor(ctx context.Context, ref resources.ActorRef) (*ateapipb.Actor, error) {
	s.mu.Lock()
	fail := s.failRead
	s.failRead = false
	s.mu.Unlock()
	if fail {
		return nil, errors.New("injected settlement read uncertainty")
	}
	return s.Interface.GetActor(ctx, ref)
}

func (s *terminationSettlementStore) UpdateActor(ctx context.Context, ref resources.ActorRef, pre store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	s.mu.Lock()
	mode := s.mode
	s.mu.Unlock()
	settlement := false
	result, err := s.Interface.UpdateActor(ctx, ref, pre, func(a *ateapipb.Actor) error {
		wasPending := a.GetStatus().GetPendingNativeOperation().GetOperation() == "terminate"
		if err := mutate(a); err != nil {
			return err
		}
		settlement = wasPending && a.GetStatus().GetPendingNativeOperation().GetOperation() == "terminate-acknowledged"
		if settlement && (mode == "write" || mode == "read" || mode == "cas") {
			if mode == "cas" {
				return store.ErrVersionConflict
			}
			return errors.New("injected settlement write uncertainty")
		}
		return nil
	})
	if settlement {
		s.mu.Lock()
		if mode == "read" {
			s.failRead = true
		}
		if mode == "cas" {
			s.mode = ""
		}
		s.mu.Unlock()
		if mode == "lost-settlement-ack" && err == nil {
			return nil, errors.New("committed settlement response lost")
		}
	}
	return result, err
}

func TestDeleteActor_TerminationSettlementBoundary(t *testing.T) {
	for _, mode := range []string{"lost-native-ack", "timeout", "write", "read", "success", "cas", "lost-settlement-ack", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			plugin := &detachFailVolumePlugin{}
			fault := &terminationSettlementStore{}
			ns := namespaceForTest("ns-delete-settlement-" + mode)
			tc := setupTestWithWrappedStore(t, ns, map[string]volume.VolumePluginControlPlane{"substrate.io/mock": plugin}, func(p store.Interface) store.Interface {
				fault.Interface = p
				return fault
			})
			defer tc.cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			createTemplateWithVolumes(t, tc, ns, []*ateapipb.Volume{{
				Name: "vol1", ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{StorageClassName: "standard", Capacity: "10Gi"},
			}}, []*ateapipb.VolumeMount{{Name: "vol1", MountPath: "/mnt/vol1"}})
			createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
			ref, worker := retirementActor(t, tc, ctx)
			// Create an actor-owned external snapshot through actual Suspend,
			// then Resume, so deletion must preserve both live volume and history.
			if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
				t.Fatal(err)
			}
			if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
				t.Fatal(err)
			}
			before, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err != nil {
				t.Fatal(err)
			}
			uri, err := resources.ParseSnapshotURI(before.GetStatus().GetExternalSnapshot().GetSnapshotUri())
			if err != nil {
				t.Fatal(err)
			}
			objects := tc.objectStore.Snapshot(t, uri)
			if len(objects) == 0 {
				t.Fatal("fixture has no actor-owned objects")
			}
			plugin.mu.Lock()
			plugin.detachAttempts = 0
			plugin.deleted = nil
			if mode == "cleanup" {
				plugin.failUntil = 1
			}
			plugin.mu.Unlock()
			tc.fakeAtelet.Lock.Lock()
			tc.fakeAtelet.TerminateCalls = 0
			if mode == "lost-native-ack" {
				tc.fakeAtelet.FailTerminate = status.Error(codes.Unavailable, "accepted termination, response lost")
			}
			if mode == "timeout" {
				tc.fakeAtelet.FailTerminate = status.Error(codes.DeadlineExceeded, "accepted termination, response timed out")
			}
			tc.fakeAtelet.Lock.Unlock()
			fault.mu.Lock()
			fault.mode = mode
			fault.mu.Unlock()
			_, err = tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true})
			unknown := mode == "lost-native-ack" || mode == "timeout" || mode == "write" || mode == "read"
			if unknown || mode == "cleanup" {
				if err == nil {
					t.Fatal("uncertain/failed cleanup reported success")
				}
				after, err := tc.client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
				if err != nil {
					t.Fatal(err)
				}
				op := after.GetStatus().GetPendingNativeOperation()
				wantOp := "terminate"
				if mode == "cleanup" {
					wantOp = "terminate-acknowledged"
				}
				if op.GetOperation() != wantOp || len(op.GetRequestDigest()) != 64 || !proto.Equal(op.GetAssignment(), before.GetStatus().GetWorkerAssignment()) || op.GetSourceWorker().GetMetadata().GetUid() != worker.GetMetadata().GetUid() {
					t.Fatalf("exact origin/ACK lost: %v", after)
				}
				if !proto.Equal(after.GetStatus().GetWorkerAssignment(), before.GetStatus().GetWorkerAssignment()) {
					t.Fatal("worker ownership released")
				}
				liveWorker := retirementReadback(t, tc, ctx, worker)
				if liveWorker.GetStatus().GetAllocated().GetActors() != 1 {
					t.Fatal("pending termination released the worker reservation")
				}
				if _, err := tc.client.DrainEmptyWorker(ctx, &ateapipb.DrainEmptyWorkerRequest{Worker: &ateapipb.ObjectRef{Name: liveWorker.GetMetadata().GetName()}, Uid: liveWorker.GetMetadata().GetUid(), Version: liveWorker.GetMetadata().GetVersion()}); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("pending termination made worker releasable: %v", err)
				}
				plugin.mu.Lock()
				detaches, deletes := plugin.detachAttempts, len(plugin.deleted)
				plugin.mu.Unlock()
				if deletes != 0 || (unknown && detaches != 0) || len(tc.objectStore.Snapshot(t, uri)) != len(objects) {
					t.Fatal("destructive cleanup escaped the termination/settlement barrier")
				}
				if unknown {
					// A new request has no positive ACK knowledge. It must hold,
					// not replay Terminate or manufacture settlement.
					restarted := controlapi.NewRPCService(tc.persistence, nil, nil, nil, nil, nil, nil, "", 10*time.Second, nil, nil, "", nil, nil)
					if _, err := restarted.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true}); status.Code(err) != codes.FailedPrecondition {
						t.Fatalf("unknown completion retried: %v", err)
					}
				} else {
					if _, err := tc.client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref, AnyState: true}); err != nil {
						t.Fatalf("settled cleanup retry failed: %v", err)
					}
				}
			} else if err != nil {
				t.Fatal(err)
			}
			tc.fakeAtelet.Lock.Lock()
			calls := tc.fakeAtelet.TerminateCalls
			tc.fakeAtelet.Lock.Unlock()
			if calls != 1 {
				t.Fatalf("native termination called %d times, want 1", calls)
			}
			if !unknown {
				plugin.mu.Lock()
				deleted := len(plugin.deleted)
				plugin.mu.Unlock()
				if deleted != 1 || len(tc.objectStore.Snapshot(t, uri)) != 0 {
					t.Fatal("settled deletion did not clean volume and actor-owned prefix")
				}
			}
		})
	}
}
