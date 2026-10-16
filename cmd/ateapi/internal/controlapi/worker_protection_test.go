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
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func protectedPod(worker *ateapipb.Worker) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: worker.GetWorkerNamespace(), Name: worker.GetWorkerPod(), UID: types.UID(worker.GetWorkerPodUid()),
			Labels: map[string]string{"ate.dev/worker-pool": worker.GetWorkerPool(), "goat.tregoat.eu/actor-active": "true"},
			Annotations: map[string]string{"karpenter.sh/do-not-disrupt": "true", "controller.kubernetes.io/pod-deletion-cost": "1000"},
		},
		Spec: corev1.PodSpec{NodeName: worker.GetNodeName()},
		Status: corev1.PodStatus{PodIP: worker.GetIp(), Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

func TestWorkerProtection_ExactLiveAuthority(t *testing.T) {
	worker := validWorker(apiWorkerName, withWorkerStatus())
	worker.Metadata.Uid = apiOtherWorkerName
	worker.Metadata.Version = 1
	for _, tc := range []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{"unprotected", func(p *corev1.Pod) { p.Annotations = nil }},
		{"replacement", func(p *corev1.Pod) { p.UID = types.UID(apiOtherWorkerName) }},
		{"namespace", func(p *corev1.Pod) { p.Namespace = "other" }},
		{"name", func(p *corev1.Pod) { p.Name = "other" }},
		{"pool", func(p *corev1.Pod) { p.Labels["ate.dev/worker-pool"] = "other" }},
		{"node", func(p *corev1.Pod) { p.Spec.NodeName = "other" }},
		{"ip", func(p *corev1.Pod) { p.Status.PodIP = "10.0.0.2" }},
		{"deleting", func(p *corev1.Pod) { now := metav1.Now(); p.DeletionTimestamp = &now }},
		{"failed", func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed }},
		{"succeeded", func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }},
		{"not-ready", func(p *corev1.Pod) { p.Status.Conditions = nil }},
		{"delete-guard", func(p *corev1.Pod) { delete(p.Labels, "goat.tregoat.eu/actor-active") }},
		{"scale-in", func(p *corev1.Pod) { delete(p.Annotations, "controller.kubernetes.io/pod-deletion-cost") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := protectedPod(worker)
			tc.change(pod)
			gate := &workerProtection{client: fake.NewClientset(pod)}
			if err := gate.verify(context.Background(), worker); err == nil {
				t.Fatal("unverified incarnation admitted")
			}
		})
	}
	gate := &workerProtection{client: fake.NewClientset(protectedPod(worker))}
	if err := gate.verify(context.Background(), worker); err != nil {
		t.Fatal(err)
	}
	unknown := proto.Clone(worker).(*ateapipb.Worker)
	unknown.Metadata.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	if err := gate.verify(context.Background(), unknown); err == nil {
		t.Fatal("unknown identity metadata admitted")
	}
	missing := proto.Clone(worker).(*ateapipb.Worker)
	missing.WorkerPodUid = ""
	if err := gate.verify(context.Background(), missing); err == nil {
		t.Fatal("missing Pod identity admitted")
	}
	gate.client.(*fake.Clientset).PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API unavailable")
	})
	if err := gate.verify(context.Background(), worker); err == nil {
		t.Fatal("authority error admitted")
	}
	var disabled *workerProtection
	if err := disabled.verify(context.Background(), nil); err != nil {
		t.Fatal("default-off gate changed existing behavior")
	}
}

// Real PG and production service/workflow admission; only Kubernetes is fake.
// This migrates an already ACTIVE worker without replacing its store record.
func TestWorkerProtection_ActiveMigrationAndResumeRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	persistence := newTestPersistence(t)
	impl := newServiceImpl(persistence, nil)
	worker := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withWorkerStatus(func(s *ateapipb.WorkerStatus) {
		s.Capacity = &ateapipb.WorkerResources{Actors: 1}
	})))
	pod := protectedPod(worker)
	pod.Annotations = nil
	kube := fake.NewClientset(pod)
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	w := &ActorWorkflow{store: impl, workerCache: wc, scheduler: scheduling.New(wc)}
	service := &RPCService{actorWorkflow: w, workerCache: wc}
	service.EnableWorkerProtection(kube)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "protected"},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	ref := resources.ActorRef{Atespace: "team-a", Name: "protected"}
	tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}}
	if _, _, err := w.assignWorkerAttempt(ctx, ref, actor, tmpl); err == nil {
		t.Fatal("ACTIVE unprotected worker admitted")
	}
	before, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil || !proto.Equal(before, worker) {
		t.Fatalf("refusal changed worker: %v", err)
	}
	if _, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, actor.GetMetadata().GetUid()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("refusal created assignment: %v", err)
	}
	pod = protectedPod(worker)
	if _, err := kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	resuming, _, err := w.assignWorkerAttempt(ctx, ref, actor, tmpl)
	if err != nil {
		t.Fatalf("same exact Pod migration did not admit: %v", err)
	}
	claim, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, actor.GetMetadata().GetUid())
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations = nil
	if _, err := kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.validateAssignedWorker(ctx, ref, resuming, tmpl); err == nil {
		t.Fatal("RESUMING retry bypassed protection")
	}
	after, err := persistence.GetActor(ctx, ref)
	if err != nil || !proto.Equal(after, resuming) {
		t.Fatalf("refusal changed actor: %v", err)
	}
	afterClaim, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, actor.GetMetadata().GetUid())
	if err != nil || !proto.Equal(afterClaim, claim) {
		t.Fatalf("refusal released/replaced reservation: %v", err)
	}
	pod = protectedPod(worker)
	if _, err := kube.CoreV1().Pods(pod.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.validateAssignedWorker(ctx, ref, resuming, tmpl); err != nil {
		t.Fatalf("protected retry refused: %v", err)
	}
}

func TestWorkerProtection_BindDrainComposition(t *testing.T) {
	for _, bindFirst := range []bool{true, false} {
		name := "drain-first"
		if bindFirst {
			name = "bind-first"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			persistence := newTestPersistence(t)
			impl := newServiceImpl(persistence, nil)
			worker := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withWorkerStatus()))
			gate := &workerProtection{client: fake.NewClientset(protectedPod(worker))}
			assignment := newAPIAssignment("protected-actor")
			if !bindFirst {
				if _, err := persistence.DrainEmptyWorker(ctx, apiWorkerName, store.PreconditionFrom(worker)); err != nil {
					t.Fatal(err)
				}
				if err := impl.BindActorToWorker(ctx, apiWorkerName, assignment, func(fresh *ateapipb.Worker) error { return gate.verify(ctx, fresh) }); err == nil {
					t.Fatal("drained worker admitted")
				}
				if _, err := persistence.GetWorkerAssignment(ctx, apiWorkerName, assignment.GetActorUid()); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("refused bind left reservation: %v", err)
				}
				return
			}
			entered, release := make(chan struct{}), make(chan struct{})
			bound := make(chan error, 1)
			go func() {
				bound <- impl.BindActorToWorker(ctx, apiWorkerName, assignment, func(fresh *ateapipb.Worker) error {
					if err := gate.verify(ctx, fresh); err != nil {
						return err
					}
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}()
			select {
			case <-entered:
			case err := <-bound:
				t.Fatalf("bind failed before admission: %v", err)
			}
			drained := make(chan error, 1)
			go func() {
				_, err := persistence.DrainEmptyWorker(ctx, apiWorkerName, store.PreconditionFrom(worker))
				drained <- err
			}()
			select {
			case err := <-drained:
				close(release)
				<-bound
				t.Fatalf("drain crossed locked admission: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			close(release)
			bindErr, drainErr := <-bound, <-drained
			if bindErr != nil {
				t.Fatal(bindErr)
			}
			if drainErr == nil {
				t.Fatal("drain succeeded after assignment")
			}
			fresh, err := persistence.GetWorker(ctx, apiWorkerName)
			if err != nil || fresh.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE || fresh.GetStatus().GetAllocated().GetActors() != 1 {
				t.Fatalf("assignment invariant lost: %v, %v", fresh, err)
			}
			if _, err := persistence.CheckWorkerAssignment(ctx, apiWorkerName, assignment.GetActorUid(), func(fresh *ateapipb.Worker) error { return gate.verify(ctx, fresh) }); err != nil {
				t.Fatalf("protected reservation lost: %v", err)
			}
		})
	}
}

func TestWorkerProtection_IndependentSiblingProductionAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	persistence := newTestPersistence(t)
	impl := newServiceImpl(persistence, nil)
	first := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName, withWorkerStatus(func(s *ateapipb.WorkerStatus) {
		s.Capacity = &ateapipb.WorkerResources{Actors: 1}
	})))
	secondInput := validWorker(apiOtherWorkerName, withWorkerStatus(func(s *ateapipb.WorkerStatus) {
		s.Capacity = &ateapipb.WorkerResources{Actors: 1}
	}))
	secondInput.WorkerPod = "worker-pod-2"
	second := seedAPIWorker(t, ctx, persistence, secondInput)
	firstPod := protectedPod(first)
	firstPod.Annotations = nil
	kube := fake.NewClientset(firstPod, protectedPod(second))
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	gate := &workerProtection{client: kube}
	w := &ActorWorkflow{
		store: impl, workerCache: wc, workerProtection: gate,
		scheduler: scheduling.New(wc, scheduling.WithIntn(func(int) int { return 0 }), scheduling.WithCandidateAdmission(gate.verify)),
	}
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "sibling"},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	tmpl := &ateapipb.ActorTemplate{SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}}
	_, chosen, err := w.assignWorkerAttempt(ctx, resources.ActorRef{Atespace: "team-a", Name: "sibling"}, actor, tmpl)
	if err != nil || chosen.GetMetadata().GetUid() != second.GetMetadata().GetUid() {
		t.Fatalf("protected sibling not allocatable: %v, %v", chosen, err)
	}
	untouched, err := persistence.GetWorker(ctx, first.GetMetadata().GetName())
	if err != nil || !proto.Equal(untouched, first) {
		t.Fatalf("unprotected sibling changed: %v", err)
	}
}
