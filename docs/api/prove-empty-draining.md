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
A native mutation (Run/Restore, checkpoint/upload or Terminate) records `ActorStatus.pending_native_operation`
**before dispatch**, including its operation and exact observed worker
UID/version, Pod/node and assignment, or the local source for a paused upload
(which has no live worker). Error, cancellation, crash and process restart
retain this origin independently of scheduling reservations. A URI or pure
workload statistics read never settles it. Successful lifecycle publication
clears pending ownership atomically with its replacement snapshot state.
Termination instead retains the exact envelope as `terminate-acknowledged`
until delete/revert cleanup finishes; it continues to protect the source node.

The request digest also pins dispatch parameters. Successful Run/Restore
settles into the live RUNNING pointer; successful Terminate settles only its
own dispatch, with the actor pointer retained through release. A failed
Terminate or NotFound is not absence of queued work.

Delete and revert refuse unresolved native ownership, including legacy
in-progress identifiers without provenance. They cannot erase the record or
delete external objects while an uncertain writer could still produce effects.
Legacy native-error crash envelopes without dispatch origin also refuse.
No automatic settlement or crash-resume path is invented. Ordinary Resume
rejects CRASHED; settled crashes still support existing revert/recovery.

Resume legitimately leaves LocalSnapshot populated. Treating it as historical
requires a complete RUNNING assignment, its durable actor-UID claim, matching
authoritative live worker/Pod/node and allocation, an assignment on a recorded
snapshot-owning node, and no pending snapshot operation. Snapshot validation
still runs. Missing/malformed pointers or claims remain unknown. The live
assignment blocks retirement of its own worker. A qualified distinct RUNNING
sibling on the same node does not block this per-worker proof; its independent
protection must still guard node retention. Pending native operations and actual
node-local checkpoints continue to block. RUNNING alone is never disposal proof.

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

## Termination settlement and retry boundary

Delete stops before detach, volume deletion, worker release and object-prefix
deletion if Terminate or durable settlement fails. After successful settlement,
it reloads the actor, checks UID and DELETING state, and verifies the raw pending
envelope through the production CAS boundary before destructive cleanup.
Detach/release failures also stop subsequent destructive cleanup.

A successful native Terminate response is positive knowledge for that exact
persisted request digest and source incarnation. The settlement CAS changes only
the envelope's operation to `terminate-acknowledged`, preserving its ownership.
A lost settlement response may reconcile by reading back that exact ACK record.
A version conflict permits one bounded retry only when UID, lifecycle state,
assignment and the entire pending origin are unchanged. Read uncertainty,
changed origin or repeated conflict refuses cleanup. No native RPC is replayed
by this reconciliation. The ACK record survives retryable later cleanup errors;
delete/revert retries skip Terminate and finish ordinary cleanup. Revert clears
the record only in its final successful lifecycle publication. Unknown fields
in the stored envelope still refuse updates. Older servers refuse the new
operation string conservatively; no database migration or wire field is added.

**Unresolved acceptance:** a process interruption after native acceptance but
before durable ACK settlement retains `terminate`, not a positive completion.
The native `TerminateRequest` has no operation-result query/token, and atelet's
Terminate first reads the sandbox record before terminating/removing it
(`cmd/atelet/main.go`, Terminate). A later NotFound does not establish that the
same request completed. Lost native ACK, timeout and such interruption therefore
hold exact origin and refuse delete/revert without destructive cleanup. Forward
recovery of genuinely unknown completion needs a separately evidenced,
exact-identity native settlement capability; this implementation does not
supply it or automatically replay unknown work.

`functionaltest/delete_settlement_test.go` exercises production DeleteActor,
PG, Resume/Suspend history, an external volume and actor-owned snapshot objects,
with faults at the fake native and real store transaction boundaries. It covers
lost ACK/timeout, settlement write/read uncertainty, bounded same-origin CAS
reconciliation, lost settlement ACK, successful cleanup and cleanup retry after
a positive ACK. These are source tests until actually executed with the required
PG/race harness.

### Allocation prerequisite and rollout

Ateapi supports opt-in `--require-worker-protection=true`. The existing Helm
`ateApi.extraArgs` passes this argument to the API container; its default empty
list and the binary's default `false` preserve existing installations. This
source job does not enable it or change deployment configuration or permissions.
All serving API replicas must enforce the gate before cleanup can be authorized.

When enabled, placement considers independently protected siblings and repeats
admission under BindActorToWorker's worker row lock, for both new claims and
rebinds. RESUMING retries use CheckWorkerAssignment under that same lock without
writing allocations or assignments. Uncertain protection does not crash an actor,
release a reservation, or delete anything. Registration may still publish ACTIVE:
ACTIVE alone is no longer sufficient to allocate when enforcement is enabled.
This deliberately covers pre-existing records without requiring a workersync
startup migration. An existing claim encountered during scheduling must also
pass the gate before any stale-claim release decision.

The authority is a direct Kubernetes Pod GET, bounded by two seconds and the
request deadline, using the existing API client and read-only Pod permission.
It must match namespace, name, UID, WorkerPool label, node and IP on a qualified
ACTIVE worker. Missing/unknown worker metadata, unavailable reads, replacement,
deletion, terminal phase or missing readiness refuse. Required metadata is
`karpenter.sh/do-not-disrupt=true`, `goat.tregoat.eu/actor-active=true`, and
`controller.kubernetes.io/pod-deletion-cost=1000`. The API neither patches these
fields nor treats cached registration or an asserted boolean as proof. Candidate
selection is advisory; the locked check is authoritative.

The PostgreSQL worker lock does not lock Kubernetes metadata. A live GET can
observe protection that the consumer subsequently removes while the worker is
still ACTIVE. In particular, consumer `6c938cbe`'s default-off path can strip
PodUID-owned or legacy protection. Its template-retention change is insufficient.
The queued consumer correction `job8f1bc30d` must retain protection for existing
exact Pods while ACTIVE, busy or unknown, even with mutation disabled. Producer
admission alone does not close that race. No global retirement protocol is added.

Safe rollout order (owner delivery requirements, not local test completion):

1. Keep producer enforcement and consumer enforcement/cleanup disabled. Deploy
   the admission capability without activating it.
2. Deploy and verify the consumer retention correction, including existing
   PodUID-owned and legacy Pods with ACTIVE native workers in default-off mode.
   Verify terminal/local-data node retention through the node-safety boundary.
3. Verify every creation template supplies protection before eligibility, then
   protect existing Pods by exact namespace/name/UID, retaining ACTIVE, unknown
   and terminal/local-data dependencies.
4. Only after retention is established, independently authorize producer
   enforcement on every API replica. Verify old/new unprotected Pods refuse
   without losing reservations and protected exact Pods and siblings admit.
5. Independently authorize consumer drain/cleanup only after its exact identity,
   cooldown, final proof, warm capacity and pending-demand checks pass. Bind and
   drain serialize on the worker row; a retained reservation blocks empty proof.
   No live flag, mutation or cleanup authorization follows from this source.

Execution of new PG/race tests, controller checks, Helm rendering and all original
checks remains required. Source coverage is not evidence that these ran. Truly
unknown native completion still requires exact-operation settlement capability;
the retained durable ownership/refusal behavior must not be weakened to recover
liveness or to obtain an empty proof.
