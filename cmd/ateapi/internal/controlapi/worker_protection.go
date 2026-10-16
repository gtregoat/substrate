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
	"net"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/scheduling"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// EnableWorkerProtection requires live, exact-incarnation protection at allocation
// admission. Configure once before serving. Disabled by default for compatibility;
// cleanup must remain disabled until every API replica enforces this prerequisite.
// The existing API service account already has read-only Pod get permission.
func (s *RPCService) EnableWorkerProtection(client kubernetes.Interface) {
	gate := &workerProtection{client: client}
	s.actorWorkflow.workerProtection = gate
	s.actorWorkflow.scheduler = scheduling.New(s.workerCache, scheduling.WithCandidateAdmission(gate.verify))
}

type workerProtection struct {
	client kubernetes.Interface
}

// verify is called inside BindActorToWorker's row lock for initial/replacement
// claims. A drain cannot commit until that claim commits or rolls back. The
// consumer may remove protection only after the exact worker is DRAINING and
// its ownership proof succeeds; DRAINING is one-way and excludes new claims.
// Resume retries retain their existing reservation, which itself blocks proof.
// This lock does not cover Pod metadata: consumer retention must be deployed
// before enforcement; see docs/api/prove-empty-draining.md.
func (g *workerProtection) verify(ctx context.Context, worker *ateapipb.Worker) error {
	if g == nil {
		return nil
	}
	refuse := func() error {
		return status.Error(codes.FailedPrecondition, "worker exact-incarnation disruption protection is unverified")
	}
	if g.client == nil || protectionUnknown(worker.ProtoReflect()) || worker.GetMetadata().GetName() == "" || worker.GetMetadata().GetAtespace() != "" || worker.GetMetadata().GetUid() == "" || worker.GetMetadata().GetVersion() < 1 || worker.GetWorkerNamespace() == "" || worker.GetWorkerPod() == "" || worker.GetWorkerPodUid() == "" || worker.GetWorkerPool() == "" || worker.GetNodeName() == "" || net.ParseIP(worker.GetIp()) == nil || worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		return refuse()
	}
	if _, err := uuid.Parse(worker.GetMetadata().GetUid()); err != nil {
		return refuse()
	}
	if _, err := uuid.Parse(worker.GetWorkerPodUid()); err != nil {
		return refuse()
	}
	if len(validation.IsDNS1123Label(worker.GetWorkerNamespace())) != 0 || len(validation.IsDNS1123Subdomain(worker.GetWorkerPod())) != 0 || len(validation.IsDNS1123Subdomain(worker.GetWorkerPool())) != 0 || len(validation.IsDNS1123Subdomain(worker.GetNodeName())) != 0 {
		return refuse()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// A direct GET deliberately bypasses informer caches and shared booleans.
	pod, err := g.client.CoreV1().Pods(worker.GetWorkerNamespace()).Get(ctx, worker.GetWorkerPod(), metav1.GetOptions{})
	if err != nil {
		return status.Error(codes.Unavailable, "cannot verify worker Pod protection")
	}
	if pod == nil || pod.Namespace != worker.GetWorkerNamespace() || pod.Name != worker.GetWorkerPod() || string(pod.UID) != worker.GetWorkerPodUid() || pod.Spec.NodeName != worker.GetNodeName() || pod.Labels["ate.dev/worker-pool"] != worker.GetWorkerPool() || pod.Status.PodIP != worker.GetIp() || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Annotations["karpenter.sh/do-not-disrupt"] != "true" || pod.Labels["goat.tregoat.eu/actor-active"] != "true" || pod.Annotations["controller.kubernetes.io/pod-deletion-cost"] != "1000" {
		return refuse()
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return nil
		}
	}
	return refuse()
}

// Unknown protobuf fields may change incarnation or admission semantics.
func protectionUnknown(message protoreflect.Message) bool {
	if !message.IsValid() {
		return false
	}
	unknown := len(message.GetUnknown()) != 0
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsMap():
			if field.MapValue().Message() != nil {
				value.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
					unknown = unknown || protectionUnknown(v.Message())
					return !unknown
				})
			}
		case field.IsList():
			if field.Message() != nil {
				for i := 0; i < value.List().Len() && !unknown; i++ {
					unknown = protectionUnknown(value.List().Get(i).Message())
				}
			}
		case field.Message() != nil:
			unknown = unknown || protectionUnknown(value.Message())
		}
		return !unknown
	})
	return unknown
}

func sameProtectionIncarnation(a, b *ateapipb.Worker) bool {
	return a.GetMetadata().GetUid() == b.GetMetadata().GetUid() && a.GetMetadata().GetName() == b.GetMetadata().GetName() && a.GetWorkerNamespace() == b.GetWorkerNamespace() && a.GetWorkerPod() == b.GetWorkerPod() && a.GetWorkerPodUid() == b.GetWorkerPodUid() && a.GetWorkerPool() == b.GetWorkerPool() && a.GetNodeName() == b.GetNodeName() && a.GetIp() == b.GetIp()
}
