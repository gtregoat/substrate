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
 "strings"

 "github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
 "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
 "github.com/google/uuid"
 "github.com/jackc/pgx/v5"
 "google.golang.org/protobuf/proto"
 "google.golang.org/protobuf/reflect/protoreflect"
)

func ownershipRefusal(reason string) error { return &store.OwnershipRefusal{Reason: reason} }

// All native ownership writers take the shared transaction lock BEFORE row
// locks. The producer takes it exclusively. It serializes only the short store
// transactions, not external operations; operation-node records cover those.
func lockOwnership(ctx context.Context, tx pgx.Tx, exclusive bool) error {
 query := `SELECT pg_advisory_xact_lock_shared(hashtextextended(current_database() || ':' || current_schema() || ':worker-ownership', 0))`
 if exclusive { query = `SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':worker-ownership', 0))` }
 _, err := tx.Exec(ctx, query)
 return err
}

func ownershipSchemaReady(ctx context.Context, tx pgx.Tx) (bool, error) {
 var ready bool
 err := tx.QueryRow(ctx, `SELECT to_regclass('worker_retirements') IS NOT NULL AND to_regclass('worker_node_operations') IS NOT NULL`).Scan(&ready)
 return ready, err
}

// Unknown fields can encode ownership dependencies unknown to this server.
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

// actorOwnershipNodes describes current native dependencies, not snapshot
// history. External snapshot URIs alone do not establish a node dependency.
func actorOwnershipNodes(actor *ateapipb.Actor) (map[string]bool, error) {
 nodes := map[string]bool{}
 if ownershipUnknown(actor.ProtoReflect()) || actor.GetStatus() == nil { return nil, ownershipRefusal(store.OwnershipUnknown) }
 status := actor.GetStatus()
 if status.GetState() < ateapipb.ActorState(1) || status.GetState() > ateapipb.ActorState(9) { return nil, ownershipRefusal(store.OwnershipUnknown) }
 if a := status.GetWorkerAssignment(); a != nil {
  if a.GetNodeName() == "" || a.GetWorker().GetName() == "" { return nil, ownershipRefusal(store.OwnershipUnknown) }
  nodes[a.GetNodeName()] = true
 }
 if local := status.GetLocalSnapshot(); local != nil {
  if local.GetSnapshotName() == "" || len(local.GetNodeVmsWithLocalSnapshots()) == 0 { return nil, ownershipRefusal(store.OwnershipUnknown) }
  for _, node := range local.GetNodeVmsWithLocalSnapshots() {
   if node == "" { return nil, ownershipRefusal(store.OwnershipUnknown) }
   nodes[node] = true
  }
 }
 if status.GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED && status.GetLocalSnapshot() == nil { return nil, ownershipRefusal(store.OwnershipUnknown) }
 if (status.GetInProgressSnapshotUri() != "" || status.GetInProgressLocalSnapshotName() != "") && len(nodes) == 0 { return nil, ownershipRefusal(store.OwnershipUnknown) }
 return nodes, nil
}

func checkNodeAdmission(ctx context.Context, tx pgx.Tx, node string) error {
 ready, err := ownershipSchemaReady(ctx, tx)
 if err != nil || !ready { return err } // existing lifecycle before capability installation
 if node == "" { return ownershipRefusal(store.OwnershipUnknown) }
 var retired bool
 if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM worker_retirements WHERE node_name = $1)`, node).Scan(&retired); err != nil { return err }
 if retired { return ownershipRefusal(store.OwnershipNodeRetired) }
 return nil
}

// recordActorOwnership runs before publication. Edges remain until the exact
// workflow token closes, even if an out-of-band actor update clears pointers.
func recordActorOwnership(ctx context.Context, tx pgx.Tx, actor *ateapipb.Actor) error {
 ready, err := ownershipSchemaReady(ctx, tx)
 if err != nil || !ready { return err }
 nodes, err := actorOwnershipNodes(actor)
 if err != nil { return err }
 md := actor.GetMetadata()
 key := "lease:actor:" + md.GetAtespace() + ":" + md.GetName()
 for node := range nodes {
  if err := checkNodeAdmission(ctx, tx, node); err != nil { return err }
  if _, err := tx.Exec(ctx, `INSERT INTO worker_node_operations (token, lease_key, node_name)
   SELECT token, key, $2 FROM leases WHERE key = $1 AND expires_at > clock_timestamp()
   ON CONFLICT DO NOTHING`, key, node); err != nil { return err }
 }
 return nil
}

func recordBindingOwnership(ctx context.Context, tx pgx.Tx, worker *ateapipb.Worker, assignment *ateapipb.ActorAssignment) error {
 if err := checkNodeAdmission(ctx, tx, worker.GetNodeName()); err != nil { return err }
 ready, err := ownershipSchemaReady(ctx, tx)
 if err != nil || !ready { return err }
 ref := assignment.GetActor()
 key := "lease:actor:" + ref.GetAtespace() + ":" + ref.GetName()
 _, err = tx.Exec(ctx, `INSERT INTO worker_node_operations (token, lease_key, node_name)
  SELECT token, key, $2 FROM leases WHERE key = $1 AND expires_at > clock_timestamp()
  ON CONFLICT DO NOTHING`, key, worker.GetNodeName())
 return err
}

// ProveEmptyDraining grants durable eligibility only after publishing a node
// admission fence in the same transaction as the authoritative ownership read.
// No worker/actor/snapshot is modified and there is no worker outbox effect.
func (p *Persistence) ProveEmptyDraining(ctx context.Context, expected *ateapipb.Worker) (*ateapipb.ProveEmptyDrainingResponse, error) {
 guard := store.PreconditionFrom(expected)
 if err := guard.Validate(); err != nil { return nil, err }
 if guard.Version < 1 || expected.GetMetadata().GetName() == "" || expected.GetWorkerNamespace() == "" || expected.GetWorkerPod() == "" || expected.GetWorkerPodUid() == "" || expected.GetNodeName() == "" { return nil, ownershipRefusal(store.OwnershipUnknown) }
 tx, err := p.pool.Begin(ctx)
 if err != nil { return nil, err }
 defer tx.Rollback(ctx) //nolint:errcheck // Commit or returned error determines the outcome.
 if err := lockOwnership(ctx, tx, true); err != nil { return nil, err }
 ready, err := ownershipSchemaReady(ctx, tx)
 if err != nil { return nil, err }
 if !ready { return nil, ownershipRefusal(store.OwnershipSchemaUnavailable) }
 worker, err := getWorkerForUpdate(ctx, tx, expected.GetMetadata().GetName())
 if err != nil { return nil, err }
 if err := guard.Check(worker.GetMetadata()); err != nil { return nil, err }
 if !sameWorkerMapping(expected, worker) { return nil, ownershipRefusal(store.OwnershipMappingChanged) }
 if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING { return nil, ownershipRefusal(store.OwnershipNotDraining) }
 if ownershipUnknown(worker.ProtoReflect()) { return nil, ownershipRefusal(store.OwnershipUnknown) }
 // Include sibling workers: removing the containing node also removes them.
 rows, err := tx.Query(ctx, `SELECT name, uid, version, proto FROM workers`)
 if err != nil { return nil, err }
 workers := map[string]*ateapipb.Worker{}
 for rows.Next() {
  var name, uid string; var version int64; var data []byte
  if err := rows.Scan(&name, &uid, &version, &data); err != nil { rows.Close(); return nil, err }
  w := &ateapipb.Worker{}
  if err := proto.Unmarshal(data, w); err != nil { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  if err := validateProtoMetadataMatchesColumns("worker "+name, w.GetMetadata(), uid, version); err != nil { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  if w.GetMetadata().GetName() != name || ownershipUnknown(w.ProtoReflect()) || w.GetNodeName() == "" { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  workers[name] = w
  if w.GetNodeName() == worker.GetNodeName() {
   a := w.GetStatus().GetAllocated()
   if w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE && w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
   if a.GetActors() != 0 || len(a.GetResources().GetLimits()) != 0 { rows.Close(); return nil, ownershipRefusal(store.OwnershipBusy) }
  }
 }
 err = rows.Err(); rows.Close(); if err != nil { return nil, err }
 rows, err = tx.Query(ctx, `SELECT worker_name FROM worker_assignments`)
 if err != nil { return nil, err }
 for rows.Next() {
  var name string
  if err := rows.Scan(&name); err != nil { rows.Close(); return nil, err }
  w, ok := workers[name]
  if !ok { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  if w.GetNodeName() == worker.GetNodeName() { rows.Close(); return nil, ownershipRefusal(store.OwnershipBusy) }
 }
 err = rows.Err(); rows.Close(); if err != nil { return nil, err }
 rows, err = tx.Query(ctx, `SELECT atespace, name, uid, version, proto FROM actors`)
 if err != nil { return nil, err }
 for rows.Next() {
  var space, name, uid string; var version int64; var data []byte
  if err := rows.Scan(&space, &name, &uid, &version, &data); err != nil { rows.Close(); return nil, err }
  actor := &ateapipb.Actor{}
  if err := proto.Unmarshal(data, actor); err != nil { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  if err := validateProtoMetadataMatchesColumns("actor "+name, actor.GetMetadata(), uid, version); err != nil { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  if actor.GetMetadata().GetName() != name || actor.GetMetadata().GetAtespace() != space { rows.Close(); return nil, ownershipRefusal(store.OwnershipUnknown) }
  nodes, err := actorOwnershipNodes(actor)
  if err != nil { rows.Close(); return nil, err }
  if nodes[worker.GetNodeName()] {
   reason := store.OwnershipBusy
   if actor.GetStatus().GetLocalSnapshot() != nil { reason = store.OwnershipLocalCheckpoint }
   if actor.GetStatus().GetInProgressSnapshotUri() != "" { reason = store.OwnershipOperationPending }
   rows.Close(); return nil, ownershipRefusal(reason)
  }
 }
 err = rows.Err(); rows.Close(); if err != nil { return nil, err }
 var pending bool
 if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM worker_node_operations WHERE node_name = $1)`, worker.GetNodeName()).Scan(&pending); err != nil { return nil, err }
 if pending { return nil, ownershipRefusal(store.OwnershipOperationPending) }
 // Row survives ordinary Worker deletion. Node-name reuse intentionally refuses;
 // no automatic expiry or destructive reconciliation is part of this capability.
 var previous []byte
 err = tx.QueryRow(ctx, `SELECT proof FROM worker_retirements WHERE node_name = $1`, worker.GetNodeName()).Scan(&previous)
 if err != nil && !errors.Is(err, pgx.ErrNoRows) { return nil, err }
 if previous != nil {
  proof := &ateapipb.ProveEmptyDrainingResponse{}
  if proto.Unmarshal(previous, proof) != nil || proof.GetOwnershipGeneration() == "" || proof.GetWorker().GetMetadata().GetUid() != guard.UID || !sameWorkerMapping(worker, proof.GetWorker()) { return nil, ownershipRefusal(store.OwnershipMappingChanged) }
  if proto.Equal(worker, proof.GetWorker()) {
   if err := tx.Commit(ctx); err != nil { return nil, err }; return proof, nil
  }
 }
 proof := &ateapipb.ProveEmptyDrainingResponse{Worker: worker, OwnershipGeneration: uuid.NewString()}
 data, err := proto.Marshal(proof); if err != nil { return nil, err }
 if _, err := tx.Exec(ctx, `INSERT INTO worker_retirements (node_name, proof) VALUES ($1, $2)
  ON CONFLICT (node_name) DO UPDATE SET proof = EXCLUDED.proof`, worker.GetNodeName(), data); err != nil { return nil, fmt.Errorf("persisting retirement fence: %w", err) }
 if err := tx.Commit(ctx); err != nil { return nil, err }
 return proof, nil
}

func sameWorkerMapping(a, b *ateapipb.Worker) bool {
 return a.GetMetadata().GetName() == b.GetMetadata().GetName() && a.GetWorkerNamespace() == b.GetWorkerNamespace() && a.GetWorkerPod() == b.GetWorkerPod() && a.GetWorkerPodUid() == b.GetWorkerPodUid() && a.GetNodeName() == b.GetNodeName() && a.GetWorkerPool() == b.GetWorkerPool()
}

// Lease acquisition snapshots dependency nodes before the workflow reads and
// performs external operations. New assignments add edges before external work.
func recordLeaseOwnership(ctx context.Context, tx pgx.Tx, key string) error {
 if !strings.HasPrefix(key, "lease:actor:") { return nil }
 parts := strings.Split(strings.TrimPrefix(key, "lease:actor:"), ":")
 if len(parts) != 2 { return ownershipRefusal(store.OwnershipUnknown) }
 var data []byte
 err := tx.QueryRow(ctx, `SELECT proto FROM actors WHERE atespace = $1 AND name = $2`, parts[0], parts[1]).Scan(&data)
 if errors.Is(err, pgx.ErrNoRows) { return nil }
 if err != nil { return err }
 actor := &ateapipb.Actor{}
 if proto.Unmarshal(data, actor) != nil { return ownershipRefusal(store.OwnershipUnknown) }
 return recordActorOwnership(ctx, tx, actor)
}
