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
 "github.com/jackc/pgx/v5"

 "github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
 "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
 "github.com/google/uuid"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/reflect/protoreflect"
)

func ownershipRefusal(reason string) error { return &store.OwnershipRefusal{Reason: reason} }

// Unknown fields may encode dependencies this server cannot understand. This
// decoder is deliberately distinct from the public discard-unknown read path.
func ownershipUnknown(m protoreflect.Message) bool {
 if len(m.GetUnknown()) != 0 { return true }
 unknown := false
 m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
  if f.IsMap() && f.MapValue().Message() != nil {
   v.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool { unknown = unknown || ownershipUnknown(v.Message()); return !unknown })
  } else if f.IsList() && f.Message() != nil {
   for i := 0; i < v.List().Len(); i++ { unknown = unknown || ownershipUnknown(v.List().Get(i).Message()) }
  } else if f.Message() != nil && !f.IsMap() && !f.IsList() { unknown = ownershipUnknown(v.Message()) }
  return !unknown
 })
 return unknown
}

func sameOwnershipMapping(a, b *ateapipb.Worker) bool {
 return a.GetMetadata().GetName() == b.GetMetadata().GetName() && a.GetWorkerNamespace() == b.GetWorkerNamespace() && a.GetWorkerPool() == b.GetWorkerPool() && a.GetWorkerPod() == b.GetWorkerPod() && a.GetWorkerPodUid() == b.GetWorkerPodUid() && a.GetNodeName() == b.GetNodeName() && a.GetIp() == b.GetIp() && a.GetSandboxClass() == b.GetSandboxClass()
}

// ProveEmptyDraining evaluates native refusals under the exact binding lock.
// Empty observations alone cannot produce success: existing lifecycle writers
// do not share a durable node retirement fence, and operation leases do not
// record node ownership after actor pointers are cleared. Until an admitted
// boundary proves those facts, the last result is STABILITY_UNPROVEN. This
// method has no mutations, events, version changes or partial-success response.
func (p *Persistence) ProveEmptyDraining(ctx context.Context, expected *ateapipb.Worker) (*ateapipb.ProveEmptyDrainingResponse, error) {
 guard := store.PreconditionFrom(expected)
 if err := guard.Validate(); err != nil { return nil, err }
 if guard.Version < 1 { return nil, store.ErrPreconditionRequired }
 if _, err := uuid.Parse(guard.UID); err != nil { return nil, store.ErrPreconditionRequired }
 if expected.GetMetadata().GetName() == "" || expected.GetMetadata().GetAtespace() != "" || expected.GetWorkerNamespace() == "" || expected.GetWorkerPool() == "" || expected.GetWorkerPod() == "" || expected.GetNodeName() == "" || expected.GetIp() == "" || ownershipUnknown(expected.ProtoReflect()) { return nil, ownershipRefusal(store.OwnershipUnknown) }
 if _, err := uuid.Parse(expected.GetWorkerPodUid()); err != nil { return nil, ownershipRefusal(store.OwnershipUnknown) }
 tx, err := p.pool.Begin(ctx)
 if err != nil { return nil, err }
 defer tx.Rollback(ctx) //nolint:errcheck // This read-only proof attempt always refuses; release the binding lock.
 var uid string; var version int64; var data []byte
 err = tx.QueryRow(ctx, `SELECT uid, version, proto FROM workers WHERE name = $1 FOR UPDATE`, expected.GetMetadata().GetName()).Scan(&uid, &version, &data)
 if errors.Is(err, pgx.ErrNoRows) { return nil, store.ErrNotFound }
 if err != nil { return nil, err }
 worker := &ateapipb.Worker{}
 if proto.Unmarshal(data, worker) != nil { return nil, ownershipRefusal(store.OwnershipUnknown) }
 if validateProtoMetadataMatchesColumns("ownership worker", worker.GetMetadata(), uid, version) != nil || ownershipUnknown(worker.ProtoReflect()) { return nil, ownershipRefusal(store.OwnershipUnknown) }
 if err := guard.Check(worker.GetMetadata()); err != nil { return nil, err }
 if !sameOwnershipMapping(expected, worker) { return nil, ownershipRefusal(store.OwnershipMappingChanged) }
 if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING || expected.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING { return nil, ownershipRefusal(store.OwnershipNotDraining) }
 allocated := worker.GetStatus().GetAllocated()
 if allocated.GetActors() != 0 || len(allocated.GetResources().GetLimits()) != 0 { return nil, ownershipRefusal(store.OwnershipBusy) }
 var assigned bool
 if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM worker_assignments WHERE worker_name = $1)`, worker.GetMetadata().GetName()).Scan(&assigned); err != nil { return nil, err }
 if assigned { return nil, ownershipRefusal(store.OwnershipBusy) }
 rows, err := tx.Query(ctx, `SELECT atespace, name, uid, version, proto FROM actors`)
 if err != nil { return nil, err }
 defer rows.Close()
 for rows.Next() {
  var space, name, uid string; var version int64; var data []byte
  if err := rows.Scan(&space, &name, &uid, &version, &data); err != nil { return nil, err }
  actor := &ateapipb.Actor{}
  if proto.Unmarshal(data, actor) != nil || ownershipUnknown(actor.ProtoReflect()) || validateProtoMetadataMatchesColumns("ownership actor", actor.GetMetadata(), uid, version) != nil || actor.GetMetadata().GetAtespace() != space || actor.GetMetadata().GetName() != name { return nil, ownershipRefusal(store.OwnershipUnknown) }
  if err := actorRetirementDependency(actor, worker); err != nil { return nil, err }
 }
 if err := rows.Err(); err != nil { return nil, err }
 // A sibling worker may bind and publish local state after this transaction
 // releases its locks. Nor can current pointers identify a former node used by
 // an in-flight operation. Neither a global history scan nor an external URI
 // establishes a stable ownership generation. Do not manufacture one.
 return nil, ownershipRefusal(store.OwnershipStabilityUnproven)
}

func actorRetirementDependency(actor *ateapipb.Actor, worker *ateapipb.Worker) error {
 s := actor.GetStatus()
 if s == nil || s.GetState() < ateapipb.ActorState(1) || s.GetState() > ateapipb.ActorState(9) { return ownershipRefusal(store.OwnershipUnknown) }
 relevant := false
 if a := s.GetWorkerAssignment(); a != nil {
  if a.GetWorker().GetName() == "" || a.GetNodeName() == "" { return ownershipRefusal(store.OwnershipUnknown) }
  relevant = a.GetWorker().GetName() == worker.GetMetadata().GetName() || a.GetNodeName() == worker.GetNodeName()
  if a.GetWorker().GetName() == worker.GetMetadata().GetName() && (a.GetNodeName() != worker.GetNodeName() || a.GetWorkerNamespace() != worker.GetWorkerNamespace() || a.GetWorkerPod() != worker.GetWorkerPod() || a.GetWorkerPodUid() != worker.GetWorkerPodUid()) { return ownershipRefusal(store.OwnershipMappingChanged) }
 }
 if local := s.GetLocalSnapshot(); local != nil {
  if local.GetSnapshotName() == "" || len(local.GetNodeVmsWithLocalSnapshots()) == 0 { return ownershipRefusal(store.OwnershipUnknown) }
  for _, node := range local.GetNodeVmsWithLocalSnapshots() {
   if node == "" { return ownershipRefusal(store.OwnershipUnknown) }
   if node == worker.GetNodeName() {
    if s.GetInProgressSnapshotUri() != "" { return ownershipRefusal(store.OwnershipOperationPending) }
    return ownershipRefusal(store.OwnershipLocalCheckpoint)
   }
  }
 }
 if s.GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED && s.GetLocalSnapshot() == nil { return ownershipRefusal(store.OwnershipUnknown) }
 if relevant {
  if s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "" { return ownershipRefusal(store.OwnershipOperationPending) }
  return ownershipRefusal(store.OwnershipBusy)
 }
 // Unlocated in-flight state cannot be attributed to an unrelated node.
 if s.GetWorkerAssignment() == nil && s.GetLocalSnapshot() == nil && (s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "") { return ownershipRefusal(store.OwnershipUnknown) }
 return nil
}
