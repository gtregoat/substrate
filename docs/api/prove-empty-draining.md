# ProveEmptyDraining native contract

`ateapi.Control/ProveEmptyDraining` is an authenticated, global-worker-scoped
per-worker retirement evidence boundary. It takes `expected_worker`, the
complete Worker from a fresh successful `GetWorker`: name, UID, positive
version, namespace, pool, Pod name/UID, node name, IP, sandbox class and
DRAINING state. Partial, stale, unknown and reused identities refuse. There
is no name-only fallback and legacy `DrainWorker` is unchanged.

## Positive evidence and its scope

The store locks the exact worker row using the production binding lock,
checks SQL/protobuf identity consistency, caller UID/version and canonical
mapping, DRAINING, zero allocations and zero durable assignments. A single
joined ownership observation reads raw actor, live worker and claim protobufs
without the public discard-unknown decoder. It refuses every dependent actor
pointer, target-node local snapshot and unresolved relevant native operation.
Corrupt, unknown, malformed or unlocated ownership is uncertainty, not empty.
Unrelated external snapshots and legitimate history do not require deletion.

Success returns the exact authoritative `worker` and an opaque
`ownership_generation` token (`observed-v1:` plus SHA-256). Despite its legacy
wire name, the token hashes actual observed worker/actor/claim records,
including their identities and versions; it is **not** a durable generation,
lease or node-wide fence. No database state, version, assignment, event or
outbox record is changed by proof. A database/commit error returns no proof.

DRAINING excludes future binds on this worker through the same row lock.
Successful pause retains the actor pointer through reservation release, then
atomically publishes LocalSnapshot/node ownership and clears the pointer.
Successful suspend/upload retains that ownership through external publication.
A native checkpoint/upload records `ActorStatus.pending_native_snapshot`
**before dispatch**, including its operation and exact observed worker
UID/version, Pod/node and assignment, or the local source for a paused upload
(which has no live worker). Error, cancellation, crash and process restart
retain this origin independently of scheduling reservations. A URI or pure
workload statistics read never settles it. Successful lifecycle publication
clears pending ownership atomically with its replacement snapshot state.

Delete and revert refuse unresolved native ownership, including legacy
in-progress identifiers without provenance. They cannot erase the record or
delete external objects while an uncertain writer could still produce effects.
No automatic settlement or crash-resume path is invented. Ordinary Resume
rejects CRASHED; settled crashes still support existing revert/recovery.

Resume legitimately leaves LocalSnapshot populated. Treating it as historical
requires a complete RUNNING assignment, its durable actor-UID claim, matching
authoritative live worker/Pod/node and allocation, an assignment on a recorded
snapshot-owning node, and no pending snapshot operation. Snapshot validation
still runs. Missing/malformed pointers or claims remain unknown. The live
assignment continues to block its node; RUNNING alone is never disposal proof.

## Consumer prerequisites before activation

This proves the exact **worker's** retirement, not unconditional authority to
delete its containing node. Another worker can become active on that node and
must independently retain protection before it becomes eligible for work.
Creation/registration protection must cover both new and pre-existing workers;
an asynchronous later annotation alone is insufficient. ACTIVE, busy, unknown
and unproved workers must remain protected. Node deletion must also account
for terminal protected Pods and local data, PVs and ordinary Pod/PDB checks.
A terminal Pod annotation alone does not establish node retention.

Goat must enforce those creation and node-safety boundaries, wire the native
producer into EmptyDrainAuthority, and preserve durable intent, owner warm
capacity and pending-demand checks. The sequence remains: drain, exact
DRAINING readback, final proof, full cooldown, identity recheck, optimistic
release of only the proven Pod's protection. No blanket pre-drain proof is
required. A new worker has independent protection and its own release proof.
Worker/Pod replacement invalidates old intent/evidence; node-name-only local
ownership conservatively blocks the current containing node and grants no
positive authority over a reused node name.

Lost drain ACK: fresh exact-incarnation DRAINING readback recovers drain state.
Lost proof ACK, errors, restart or unavailable reads: preserve protection,
re-read identities and obtain a new complete proof. DRAINING readback alone
is never ownership evidence. Any absent/partial/mismatched success envelope,
empty token, unknown response field or UNIMPLEMENTED is refusal. Consumers
must not substitute list counts or GetActiveWorkloadStats.

## Wire refusals

* `INVALID_ARGUMENT`: malformed request, missing UID/nonpositive version.
* `NOT_FOUND`: missing worker; ownership unknown.
* `ABORTED`: UID/version changed, including name reuse; read again.
* `FAILED_PRECONDITION`: native refusal. `google.rpc.ErrorInfo` has domain
  `ateapi.worker-ownership` and a reason below.

| Reason | Missing or conflicting fact |
| --- | --- |
| `OWNERSHIP_NOT_DRAINING` | Expected/current worker is not DRAINING |
| `OWNERSHIP_MAPPING_CHANGED` | Canonical Pod/node mapping differs |
| `OWNERSHIP_BUSY` | Allocation, assignment or dependent actor pointer remains |
| `OWNERSHIP_LOCAL_CHECKPOINT` | A current checkpoint requires the containing node |
| `OWNERSHIP_OPERATION_PENDING` | Relevant unresolved native work remains |
| `OWNERSHIP_UNKNOWN` | Partial, unknown, corrupt or unlocated ownership |
| `OWNERSHIP_STABILITY_UNPROVEN` | Reserved refusal from older producer versions |

Every error returns no proof response. Unknown reasons also retain protection.

Native implementation/test source is not deployed acceptance. Mission Control
owns publication and subsequent review, CI, Kargo deployment, Goat integration
and deployed acceptance. The default-off live guard is unchanged. See the
[remaining integration/evidence boundary](worker-retirement-evidence-gap.md).
