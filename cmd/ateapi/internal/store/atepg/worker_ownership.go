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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/apimachinery/pkg/util/validation"
)

func ownershipRefusal(reason string) error { return &store.OwnershipRefusal{Reason: reason} }

// Unknown fields may encode dependencies this server cannot understand. This
// decoder is deliberately distinct from the public discard-unknown read path.
func ownershipUnknown(m protoreflect.Message) bool {
	if len(m.GetUnknown()) != 0 {
		return true
	}
	unknown := false
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.IsMap() && f.MapValue().Message() != nil {
			v.Map().Range(func(_ protoreflect.MapKey, v protoreflect.Value) bool {
				unknown = unknown || ownershipUnknown(v.Message())
				return !unknown
			})
		} else if f.IsList() && f.Message() != nil {
			for i := 0; i < v.List().Len(); i++ {
				unknown = unknown || ownershipUnknown(v.List().Get(i).Message())
			}
		} else if f.Message() != nil && !f.IsMap() && !f.IsList() {
			unknown = ownershipUnknown(v.Message())
		}
		return !unknown
	})
	return unknown
}

func sameOwnershipMapping(a, b *ateapipb.Worker) bool {
	return a.GetMetadata().GetName() == b.GetMetadata().GetName() && a.GetWorkerNamespace() == b.GetWorkerNamespace() && a.GetWorkerPool() == b.GetWorkerPool() && a.GetWorkerPod() == b.GetWorkerPod() && a.GetWorkerPodUid() == b.GetWorkerPodUid() && a.GetNodeName() == b.GetNodeName() && a.GetIp() == b.GetIp() && a.GetSandboxClass() == b.GetSandboxClass()
}

// ProveEmptyDraining returns per-worker retirement evidence under the binding
// lock. DRAINING excludes new binds on this incarnation. Atomic lifecycle
// publication conserves its pointer, local ownership, or pending native origin.
// Other workers require their own creation/ACTIVE protection and final proof;
// this evidence never authorizes unconditional containing-node retirement.
func (p *Persistence) ProveEmptyDraining(ctx context.Context, expected *ateapipb.Worker) (*ateapipb.ProveEmptyDrainingResponse, error) {
	guard := store.PreconditionFrom(expected)
	if err := guard.Validate(); err != nil {
		return nil, err
	}
	if guard.Version < 1 {
		return nil, store.ErrPreconditionRequired
	}
	if _, err := uuid.Parse(guard.UID); err != nil {
		return nil, store.ErrPreconditionRequired
	}
	if expected.GetMetadata().GetName() == "" || expected.GetMetadata().GetAtespace() != "" || expected.GetWorkerNamespace() == "" || expected.GetWorkerPool() == "" || expected.GetWorkerPod() == "" || expected.GetNodeName() == "" || net.ParseIP(expected.GetIp()) == nil || len(validation.IsDNS1123Label(expected.GetWorkerNamespace())) != 0 || len(validation.IsDNS1123Subdomain(expected.GetWorkerPool())) != 0 || len(validation.IsDNS1123Subdomain(expected.GetWorkerPod())) != 0 || len(validation.IsDNS1123Subdomain(expected.GetNodeName())) != 0 || ownershipUnknown(expected.ProtoReflect()) {
		return nil, ownershipRefusal(store.OwnershipUnknown)
	}
	if _, err := uuid.Parse(expected.GetWorkerPodUid()); err != nil {
		return nil, ownershipRefusal(store.OwnershipUnknown)
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // Release the binding lock on every refusal/error.
	var uid string
	var version int64
	var data []byte
	err = tx.QueryRow(ctx, `SELECT uid, version, proto FROM workers WHERE name = $1 FOR UPDATE`, expected.GetMetadata().GetName()).Scan(&uid, &version, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	worker := &ateapipb.Worker{}
	if proto.Unmarshal(data, worker) != nil {
		return nil, ownershipRefusal(store.OwnershipUnknown)
	}
	if worker.GetMetadata().GetAtespace() != "" || validateProtoMetadataMatchesColumns("ownership worker", worker.GetMetadata(), uid, version) != nil || ownershipUnknown(worker.ProtoReflect()) {
		return nil, ownershipRefusal(store.OwnershipUnknown)
	}
	if err := guard.Check(worker.GetMetadata()); err != nil {
		return nil, err
	}
	if !sameOwnershipMapping(expected, worker) {
		return nil, ownershipRefusal(store.OwnershipMappingChanged)
	}
	if worker.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING || expected.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		return nil, ownershipRefusal(store.OwnershipNotDraining)
	}
	allocated := worker.GetStatus().GetAllocated()
	if allocated.GetActors() != 0 || len(allocated.GetResources().GetLimits()) != 0 {
		return nil, ownershipRefusal(store.OwnershipBusy)
	}
	var assigned bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM worker_assignments WHERE worker_name = $1)`, worker.GetMetadata().GetName()).Scan(&assigned); err != nil {
		return nil, err
	}
	if assigned {
		return nil, ownershipRefusal(store.OwnershipBusy)
	}
	// Hash actual observed records, not a synthetic durable generation.
	evidence := sha256.New()
	fmt.Fprintf(evidence, "worker-retirement-v1:%d:", len(data))
	evidence.Write(data)
	rows, err := tx.Query(ctx, `SELECT a.atespace, a.name, a.uid, a.version, a.proto,
        w.proto, COALESCE(w.name, ''), COALESCE(w.uid::text, ''), COALESCE(w.version, 0), wa.proto
        FROM actors a
        LEFT JOIN worker_assignments wa ON wa.actor_uid = a.uid
        LEFT JOIN workers w ON w.name = wa.worker_name
        ORDER BY a.atespace, a.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var space, name, uid string
		var version int64
		var data, liveData, claimData []byte
		var liveName, liveUID string
		var liveVersion int64
		if err := rows.Scan(&space, &name, &uid, &version, &data, &liveData, &liveName, &liveUID, &liveVersion, &claimData); err != nil {
			return nil, err
		}
		if _, err := uuid.Parse(uid); err != nil || version < 1 || len(validation.IsDNS1123Label(space)) != 0 || len(validation.IsDNS1123Label(name)) != 0 {
			return nil, ownershipRefusal(store.OwnershipUnknown)
		}
		actor := &ateapipb.Actor{}
		if proto.Unmarshal(data, actor) != nil || ownershipUnknown(actor.ProtoReflect()) || validateProtoMetadataMatchesColumns("ownership actor", actor.GetMetadata(), uid, version) != nil || actor.GetMetadata().GetAtespace() != space || actor.GetMetadata().GetName() != name {
			return nil, ownershipRefusal(store.OwnershipUnknown)
		}
		var live *ateapipb.Worker
		var claim *ateapipb.ActorAssignment
		if len(liveData) != 0 && len(claimData) != 0 {
			live = &ateapipb.Worker{}
			claim = &ateapipb.ActorAssignment{}
			if proto.Unmarshal(liveData, live) != nil || proto.Unmarshal(claimData, claim) != nil ||
				ownershipUnknown(live.ProtoReflect()) || ownershipUnknown(claim.ProtoReflect()) || live.GetMetadata().GetName() != liveName ||
				validateProtoMetadataMatchesColumns("live ownership worker", live.GetMetadata(), liveUID, liveVersion) != nil {
				return nil, ownershipRefusal(store.OwnershipUnknown)
			}
		}
		if err := actorRetirementDependency(actor, worker, live, claim); err != nil {
			return nil, err
		}
		for _, observation := range [][]byte{data, liveData, claimData} {
			fmt.Fprintf(evidence, "%d:", len(observation))
			evidence.Write(observation)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &ateapipb.ProveEmptyDrainingResponse{
		Worker:              worker,
		OwnershipGeneration: "observed-v1:" + hex.EncodeToString(evidence.Sum(nil)),
	}, nil
}

func actorRetirementDependency(actor *ateapipb.Actor, worker, live *ateapipb.Worker, claim *ateapipb.ActorAssignment) error {
	s := actor.GetStatus()
	if s == nil || s.GetState() < ateapipb.ActorState(1) || s.GetState() > ateapipb.ActorState(9) {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	if err := nativeOperationDependency(actor, worker); err != nil {
		return err
	}
	relevant := false
	if a := s.GetWorkerAssignment(); a != nil {
		if !qualifiedAssignmentCoordinates(a) {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		relevant = a.GetWorker().GetName() == worker.GetMetadata().GetName() || a.GetNodeName() == worker.GetNodeName()
		if a.GetWorker().GetName() == worker.GetMetadata().GetName() && (a.GetNodeName() != worker.GetNodeName() || a.GetWorkerNamespace() != worker.GetWorkerNamespace() || a.GetWorkerPod() != worker.GetWorkerPod() || a.GetWorkerPodUid() != worker.GetWorkerPodUid()) {
			return ownershipRefusal(store.OwnershipMappingChanged)
		}
	}
	// Resume retains LocalSnapshot. Only a qualified live assignment can make
	// it historical; a state enum or denormalized pointer alone is insufficient.
	historical := false
	if s.GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		if !qualifiedRunningAssignment(actor, live, claim) {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		historical = true
		// A qualified distinct worker retains its own live assignment. Shared
		// placement alone is not a dependency on this worker's retirement;
		// independent sibling protection is the consumer's node-safety guard.
		relevant = s.GetWorkerAssignment().GetWorker().GetName() == worker.GetMetadata().GetName()
	}
	if local := s.GetLocalSnapshot(); local != nil {
		if len(validation.IsDNS1123Label(local.GetSnapshotName())) != 0 || len(local.GetNodeVmsWithLocalSnapshots()) == 0 {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		seen := make(map[string]bool)
		for _, node := range local.GetNodeVmsWithLocalSnapshots() {
			if seen[node] {
				return ownershipRefusal(store.OwnershipUnknown)
			}
			seen[node] = true
			if len(validation.IsDNS1123Subdomain(node)) != 0 {
				return ownershipRefusal(store.OwnershipUnknown)
			}
			if node == worker.GetNodeName() && !historical {
				if s.GetInProgressSnapshotUri() != "" {
					return ownershipRefusal(store.OwnershipOperationPending)
				}
				return ownershipRefusal(store.OwnershipLocalCheckpoint)
			}
		}
	}
	if s.GetState() == ateapipb.ActorState_ACTOR_STATE_PAUSED && s.GetLocalSnapshot() == nil {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	if relevant {
		if s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "" {
			return ownershipRefusal(store.OwnershipOperationPending)
		}
		return ownershipRefusal(store.OwnershipBusy)
	}
	if store.UnlocatedNativeFailure(s) {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	// Unlocated in-flight state cannot be attributed to an unrelated node.
	if s.GetPendingNativeOperation() == nil && s.GetWorkerAssignment() == nil && (s.GetLocalSnapshot() == nil || s.GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDING) && (s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "") {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	return nil
}

// qualifiedRunningAssignment requires the durable claim and its authoritative
// worker from the same database observation as the actor. This is qualification
// of historical metadata, not a node-retirement fence or operation settlement.
func qualifiedRunningAssignment(actor *ateapipb.Actor, live *ateapipb.Worker, claim *ateapipb.ActorAssignment) bool {
	s := actor.GetStatus()
	a := s.GetWorkerAssignment()
	if a == nil || live == nil || claim == nil || s.GetPendingNativeOperation() != nil || s.GetInProgressSnapshotUri() != "" || s.GetInProgressLocalSnapshotName() != "" {
		return false
	}
	if a.GetWorker().GetName() == "" || a.GetWorker().GetAtespace() != "" || a.GetWorkerNamespace() == "" || a.GetWorkerPool() == "" || a.GetWorkerPod() == "" || a.GetNodeName() == "" || net.ParseIP(a.GetWorkerPodIp()) == nil {
		return false
	}
	if _, err := uuid.Parse(a.GetWorkerPodUid()); err != nil {
		return false
	}
	if _, err := uuid.Parse(live.GetMetadata().GetUid()); err != nil {
		return false
	}
	if s.GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING || live.GetMetadata().GetAtespace() != "" || live.GetMetadata().GetVersion() < 1 || live.GetStatus().GetAllocated().GetActors() < 1 || (live.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE && live.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING) {
		return false
	}
	if claim.GetActorUid() == "" || claim.GetActorUid() != actor.GetMetadata().GetUid() || claim.GetActor().GetName() != actor.GetMetadata().GetName() || claim.GetActor().GetAtespace() != actor.GetMetadata().GetAtespace() {
		return false
	}
	if a.GetWorker().GetName() != live.GetMetadata().GetName() || a.GetWorkerNamespace() != live.GetWorkerNamespace() || a.GetWorkerPool() != live.GetWorkerPool() || a.GetWorkerPod() != live.GetWorkerPod() || a.GetWorkerPodUid() != live.GetWorkerPodUid() || a.GetNodeName() != live.GetNodeName() || a.GetWorkerPodIp() != live.GetIp() {
		return false
	}
	if local := s.GetLocalSnapshot(); local != nil {
		ownsSnapshot := false
		for _, node := range local.GetNodeVmsWithLocalSnapshots() {
			ownsSnapshot = ownsSnapshot || node == a.GetNodeName()
		}
		if !ownsSnapshot {
			return false
		}
	}
	return true
}

// nativeOperationDependency uses persisted dispatch provenance, including after
// crash releases the scheduling reservation. A URI alone is never settlement.
func nativeOperationDependency(actor *ateapipb.Actor, target *ateapipb.Worker) error {
	s := actor.GetStatus()
	op := s.GetPendingNativeOperation()
	if op == nil {
		return nil
	}
	state := s.GetState()
	validState := state == ateapipb.ActorState_ACTOR_STATE_CRASHED ||
		op.GetOperation() == "checkpoint-local" && state == ateapipb.ActorState_ACTOR_STATE_PAUSING ||
		(op.GetOperation() == "checkpoint-external" || op.GetOperation() == "upload-local") && state == ateapipb.ActorState_ACTOR_STATE_SUSPENDING ||
		(op.GetOperation() == "run" || op.GetOperation() == "restore") && state == ateapipb.ActorState_ACTOR_STATE_RESUMING ||
		op.GetOperation() == "terminate" && (state == ateapipb.ActorState_ACTOR_STATE_DELETING || state == ateapipb.ActorState_ACTOR_STATE_REVERTING)
	if !validState {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	if digest, err := hex.DecodeString(op.GetRequestDigest()); err != nil || len(digest) != sha256.Size {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	if op.GetLocalSnapshotName() != s.GetInProgressLocalSnapshotName() || op.GetSnapshotUri() != s.GetInProgressSnapshotUri() {
		return ownershipRefusal(store.OwnershipUnknown)
	}
	switch op.GetOperation() {
	case "checkpoint-local", "checkpoint-external", "run", "restore", "terminate":
		source, a := op.GetSourceWorker(), op.GetAssignment()
		if source == nil || !qualifiedAssignmentCoordinates(a) || op.GetLocalSource() != nil || a.GetWorker().GetName() == "" || a.GetWorker().GetAtespace() != "" || source.GetMetadata().GetAtespace() != "" || source.GetMetadata().GetVersion() < 1 || source.GetWorkerNamespace() == "" || source.GetWorkerPool() == "" || source.GetWorkerPod() == "" || source.GetNodeName() == "" || net.ParseIP(source.GetIp()) == nil {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		if source.GetStatus().GetAllocated().GetActors() < 1 || (source.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE && source.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING) {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		if _, err := uuid.Parse(source.GetMetadata().GetUid()); err != nil {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		if _, err := uuid.Parse(source.GetWorkerPodUid()); err != nil {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		if a.GetWorker().GetName() != source.GetMetadata().GetName() || a.GetNodeName() != source.GetNodeName() || a.GetWorkerNamespace() != source.GetWorkerNamespace() || a.GetWorkerPool() != source.GetWorkerPool() || a.GetWorkerPod() != source.GetWorkerPod() || a.GetWorkerPodUid() != source.GetWorkerPodUid() || a.GetWorkerPodIp() != source.GetIp() {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		if (op.GetOperation() == "checkpoint-local" && op.GetLocalSnapshotName() == "") || (op.GetOperation() == "checkpoint-external" && op.GetSnapshotUri() == "") {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		if source.GetMetadata().GetUid() == target.GetMetadata().GetUid() && !sameOwnershipMapping(source, target) || source.GetWorkerPodUid() == target.GetWorkerPodUid() && source.GetNodeName() != target.GetNodeName() {
			return ownershipRefusal(store.OwnershipMappingChanged)
		}
		if source.GetMetadata().GetUid() == target.GetMetadata().GetUid() || source.GetNodeName() == target.GetNodeName() {
			return ownershipRefusal(store.OwnershipOperationPending)
		}
	case "upload-local":
		local := op.GetLocalSource()
		if op.GetSourceWorker() != nil || op.GetAssignment() != nil || op.GetSnapshotUri() == "" || local.GetSnapshotName() == "" || len(local.GetNodeVmsWithLocalSnapshots()) == 0 || !proto.Equal(local, s.GetLocalSnapshot()) {
			return ownershipRefusal(store.OwnershipUnknown)
		}
		seen := make(map[string]bool)
		for _, node := range local.GetNodeVmsWithLocalSnapshots() {
			if seen[node] {
				return ownershipRefusal(store.OwnershipUnknown)
			}
			seen[node] = true
			if len(validation.IsDNS1123Subdomain(node)) != 0 {
				return ownershipRefusal(store.OwnershipUnknown)
			}
			if node == target.GetNodeName() {
				return ownershipRefusal(store.OwnershipOperationPending)
			}
		}
	default:
		return ownershipRefusal(store.OwnershipUnknown)
	}
	return nil
}

func qualifiedAssignmentCoordinates(a *ateapipb.WorkerAssignment) bool {
	if a == nil || a.GetWorker().GetName() == "" || a.GetWorker().GetAtespace() != "" || len(validation.IsDNS1123Label(a.GetWorkerNamespace())) != 0 || len(validation.IsDNS1123Subdomain(a.GetWorkerPool())) != 0 || len(validation.IsDNS1123Subdomain(a.GetWorkerPod())) != 0 || len(validation.IsDNS1123Subdomain(a.GetNodeName())) != 0 || net.ParseIP(a.GetWorkerPodIp()) == nil {
		return false
	}
	_, err := uuid.Parse(a.GetWorkerPodUid())
	return err == nil
}
