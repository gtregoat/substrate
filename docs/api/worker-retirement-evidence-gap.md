# Worker retirement evidence and remaining consumer boundary

The current producer has an exact-incarnation positive branch and retained
native snapshot provenance. Its scope and required consumer composition are
specified in [the API contract](prove-empty-draining.md). No node-wide durable
fence, history deletion, live cleanup, consumer activation or Goat source change
is part of this native component.

## Current source and validation boundaries

The assigned branch starts at authorized main
`74f71ca7bbe8dc9516f4498dd1027d017f537001` and preserves retained head
`3ddfa260814962fb9469b177ca8ecf1f4eff3d59` (including a14 and 764) as an ancestor.
Canonical088 FAILED at 23:42:01 and its original native task later completed
at 23:50:54 on 3dd. Those are separate historical facts; neither is rewritten.
The late report was PARTLY_DONE, and its missing-tool executions do not validate
this correction. The historical node-wide-fence argument below is retained as
history, not a current producer requirement or a proved production race.

Producer source includes tests for exact positive/refusal effects, raw
unknown ownership, both binding orders, lost-result identity readback,
production Bind/Pause/Resume/upload composition, reservation-to-local handoff,
and ambiguous checkpoint failure before/after accepted local work. Test source
is not execution evidence. Required CGO/race/PostgreSQL, codegen and `make
verify` outcomes must be reported for the exact pushed SHA; missing tools and
skips are never acceptance. No older a14 run validates this source.

## Consumer work and release evidence still owned by Mission Control

Before activation, Goat must enforce protection at worker creation/eligibility
(including already unprotected Pods), preserve ACTIVE/unknown protection, and
include protected terminal/local-data Pods in node-deletion safety. It must wire
native evidence into the existing durable drain intent/readback/proof/cooldown
flow with exact identity rechecks, owner warm capacity and pending demand.
Its already protected, newly created/racing, and protected terminal sibling
cases must run through actual creation/registration and deletion predicates.
Native source alone does not close those separate component boundaries.

Newly recorded uncertain native operations preserve exact dispatch origin.
Delete/revert deliberately refuse until completion is separately evidenced;
there is no speculative settlement by statistics, external URI presence or
Pod disappearance. Legacy unlocated in-progress records also refuse. A
future reconciliation mechanism must establish native lifecycle settlement
before clearing them; it cannot discard data/history to manufacture eligibility.

## Archived pre-correction investigation (superseded)

The following text records the prior refusal-only implementation and its design
inference. Its unconditional-refusal status and proposed node-wide fence are
superseded by the current per-worker contract and bounded provenance fix.

# Worker retirement evidence: unresolved native boundary

Status: the native RPC/store typed-refusal path is implemented; stable positive
ownership evidence remains unimplemented. See [the consumer contract](prove-empty-draining.md).
`DrainEmptyWorker` is not retirement evidence. All current proof results refuse
release, and the existing live guard remains unchanged.

## Source-preserving starting point

The task branch merges reviewed commit
`a14d6854471f67cfbd662875719d1f99ab8704e6` onto
`74f71ca7bbe8dc9516f4498dd1027d017f537001`. Both are ancestors. The existing
atomic drain implementation and fixture assertions are preserved.

## Observed ownership boundaries

* `atepg/worker_assignment.go`: `BindActorToWorker` and
  `ReleaseActorFromWorker` lock the worker row. Admission runs under that lock.
  `DrainEmptyWorker` uses the same lock, checks assignments, allocation and
  actor worker pointers, and changes ACTIVE to DRAINING. This fences assignment
  to the exact worker; it does not retire its containing node.
* `controlapi/workflow_pause.go`, `ensurePausedFinalized`: worker reservation
  release precedes a separate actor update. That actor update publishes PAUSED,
  the local snapshot's node name and the cleared worker pointer together.
  Between reservation release and that update the actor worker pointer remains
  relevant. After that update the local snapshot remains relevant even though
  no worker pointer remains.
* `atepg/actor.go`: `UpdateActor` reads and performs a UID/version conditional
  update. It does not acquire the worker row lock. `CreateActor` also has no
  worker/node lock. Holding the drained worker lock while scanning actor rows
  therefore does not serialize all ownership publication.
* `controlapi/workflow_suspend.go`, `ensurePausedSnapshotUploaded`: the upload
  is sent to the atelet on the recorded local snapshot node. A destination URI
  in `InProgressSnapshotUri` is not upload completion. The legitimate transition
  in `ensureSuspendedFinalized` commits the external snapshot, clears local
  snapshot and assignment, and records SUSPENDED. External snapshots alone must
  not force an otherwise unrelated worker to remain protected.
* `Worker` records namespace, Pod name, Pod UID and node name as immutable
  coordinates. It does not record a node UID or a node retirement generation.
  Matching these stored strings is not independent evidence that a current
  Kubernetes node incarnation is the same one.

## Requirements for a supported producer

These requirements distinguish full release evidence from the implemented
refusal path. Identity validation and current dependency refusals are present;
stable positive evidence, lifecycle admission fences and full acceptance remain
outstanding:

1. Authenticate and validate exact caller-observed worker UID/version and
   canonical Pod/node coordinates. Refuse ACTIVE, stale/reused identity,
   mapping drift, unknown/corrupt ownership and partial reads with typed errors.
2. Preserve drain-before-proof ordering. Recheck DRAINING and empty allocation,
   assignment and actor pointers under the production binding lock.
3. Establish a durable ownership fence covering the containing node's relevant
   dependencies. A sibling worker can publish new node-local checkpoints even
   while this worker is DRAINING. A scan, actor row locks, or a transaction-only
   lock cannot prevent new dependencies after the proof transaction commits.
   The producer and all relevant admission/lifecycle writers must share the
   fence, including publication already in flight at proof time. This also
   needs explicit review of volume attachment and operation ownership paths.
4. Refuse local-only checkpoints and pending relevant uploads/operations.
   Permit independently retained external data and unrelated historical actors;
   do not require globally empty actor or snapshot history. Do not delete or
   migrate data to obtain proof.
5. Define the ownership generation and its validity through ordinary release,
   including lost-result readback, restart, name reuse and mapping changes.
   Do not issue an unfenced boolean or infer retention from an object URI.
6. Test the real control/store boundaries and PostgreSQL interleavings: exact
   empty success, identity/mapping failures, local checkpoint refusal, in-flight
   upload refusal, legitimate external-retention transition and concurrent
   dependency publication in both orders. Keep the existing drain tests intact.

No new schema, controller, attestation service or lifecycle fence is introduced
by this investigation. Stable positive proof and its full new race acceptance
remain unfinished. Review/publication, CI, Kargo deployment, capability rollout,
Goat integration and deployed acceptance remain owner delivery work; this
source does not authorize or demonstrate live cleanup.

## Blocking schedule and authority boundary

A concrete interleaving demonstrates the missing fact:

1. Empty worker W on node N becomes DRAINING. A proof transaction locks W and
   observes no current node-local dependencies.
2. That transaction commits. Sibling worker S on N remains ACTIVE and its
   independent worker row still admits a new binding.
3. S's actor pauses and publishes a local checkpoint on N through `UpdateActor`.
4. Releasing W's protection can now allow ordinary removal of N to destroy that
   useful checkpoint. Locking the actor/worker tables only during step 1 does
   not prevent step 3 after commit.

Keeping the fence solely in W cannot bridge ordinary removal either:
`cmd/atecontroller/internal/workersync/syncer.go`, `reconcileDeadWorker`, invokes
`DeleteWorker` when the Pod disappears. `workflow_worker_delete.go` clears actor
pointers and ultimately removes the Worker row. A fence that survives that
lifecycle needs supported persistent node ownership state. Existing workflow
leases record key/token/expiry but not the node dependencies of an operation
whose actor pointer has subsequently been cleared.

The task's later constraint-conflict steering explicitly prohibits any new
durable schema/fence and says to report missing authority as a blocker. No
migration, permanent lease tombstone, retained deleted Worker or transient lock
presented as durable evidence is shipped. The later underdelivery steering
authorizes a concrete typed-refusal RPC/store path when native stability facts
remain unprovable. That path is implemented, including STABILITY_UNPROVEN for
an empty candidate; it is explicitly not the completed positive proof.

`TestDrainEmptyWorker_DoesNotCertifyNodeRetirement` adds two real PostgreSQL
store cases, using the existing migrated fixture: local checkpoint publication
before drain, and sibling admission/local checkpoint publication after drain.
Both assert that the target remains empty and DRAINING while the actor retains
its node-local checkpoint. They document the native boundary's insufficiency;
they are not producer acceptance, an atelet upload test, or a claim of a passing
SQL execution. No existing drain assertions are weakened.

Completion requires owner-admitted support for the durable node and in-flight
operation ownership facts (or an identified existing complete native boundary),
then the stable positive producer, lifecycle admission hooks and
all remaining requested positive/race tests. The generated RPC and typed
refusal store path can already be reviewed independently. Tool availability does not remove
that implementation requirement. The added documentation is outside the
original write list because it records this exact ownership-evidence blocker.

## Exact-Pod allocation gate continuation

The opt-in producer prerequisite is implemented in
`cmd/ateapi/internal/controlapi/worker_protection.go`, the production Resume
workflow, and the worker-row-locked `CheckWorkerAssignment` store operation.
It uses existing read-only Kubernetes authority and requires no new protocol,
RBAC or controller. The rollout contract is in `prove-empty-draining.md`.
Workersync's registration readiness remains advisory; the API admission gate
covers both fresh and already ACTIVE records before allocation or resume retry.

The source adds real PG/service/workflow tests with fake external Kubernetes
only, including migration of the same ACTIVE incarnation, reservation-preserving
resume refusal/retry, sibling placement, and bind/drain exclusion. These are
source coverage until actually executed. No Go/gofmt, gcc, Docker, make, unzip
or Helm executable was found on this sandbox PATH at implementation time.
All mandatory final commands and base comparisons must be reported as actually
returned, without inheriting any earlier source's execution evidence. Complete
PG/race/lifecycle acceptance, generated-code/format verification and rendered
configuration checks remain outstanding if tools are unavailable. Unknown
native completion remains durably held; exact-operation remote reconciliation
is still the explicit retained protocol gap, not a fabricated successful recovery.

## Current continuation: exact-operation capability gap

The following is the current boundary; the earlier investigation's permanently
refusing proof/fence discussion remains historical. Positive proof exists, but
complete remote operation recovery does not.

`internal/proto/ateletpb/atelet.proto` exposes Run, Restore, Checkpoint,
UploadPausedCheckpoint and Terminate without a caller operation token or result
lookup. Their responses are empty. Actor UID and target ateom UID identify the
workload/incarnation, not an invocation: an actor can pause/resume repeatedly on
the same Pod. Snapshot names and destination URIs identify outputs but do not
prove all cleanup has settled. In `cmd/atelet/main.go`, Checkpoint performs local
and external phases; UploadPausedCheckpoint uploads then prunes local files;
Terminate reads sandbox assets before termination, unmount and directory removal.
A missing sandbox or a present remote snapshot is not an exact completion receipt.
GetActiveWorkloadStats supplies neither a lifecycle fence nor operation identity.
There is no existing authoritative query this producer can call to settle these
operations after a process interruption or lost native response.

The producer now refuses redispatch even when the persisted request digest
matches. In particular Resume checks pending ownership before volume attachment;
checkpoint/upload dispatch also refuses an existing origin. This prevents a
restart from replaying remote effects or clearing the first dispatch origin.
A positive Terminate ACK still uses the existing bounded same-operation CAS
reconciliation and survives later cleanup failures. DeleteActor's same-call
settlement barrier is retained. Unknown completion remains protected and is an
explicit liveness gap, never a success or evidence of absence.

The smallest compatible native extension is additive: a caller-generated
operation ID persisted with the actor's request digest and exact source
incarnation, carried on each lifecycle request, and an authenticated result
lookup keyed by actor UID, target incarnation, operation ID and request digest.
Native dispatch must durably record acceptance before effects, reject token
reuse with a different request, serialize effects with lifecycle work, and retain
a terminal receipt independently of sandbox/local-checkpoint deletion. Lookup
must distinguish unknown/in-flight from terminal success with its result and
settled local-data disposition; NotFound cannot imply success. Capability absence
on older servers must retain ownership with no fallback. Producer reconciliation
would validate the exact receipt, CAS the matching actor operation, and resume
only the corresponding publication/cleanup. A producer-only digest or timeout
cannot implement this native durability contract. That extension and its real
native restart acceptance are unimplemented, rather than represented by a fake
receipt or a permanently refusing path labeled complete.

Consumer retention correction source is now reported at Goat
`3800b186e7913a9a886b585da9261cdebfcd2602` (`agent/mc-8f1bc30d`): root reports
production RequireAuthority and nonrace retention regressions. This is unmerged,
undeployed cross-job evidence, not this sandbox's test result. The producer's
GET/row lock cannot prevent an old sampled-count remover from stripping Pod
metadata. Deploy and verify that retention correction before enabling producer
enforcement or drain; retain all creation and terminal/local-data prerequisites
and the safe rollout sequence in the API contract.

This continuation adds gated production RPC/PG lifecycle source coverage in
`functionaltest/protected_lifecycle_test.go`, plus a pending-run publication
failure regression. External native work alone is faked. It requires the real
PG fixture and envtest; compilation alone and skipped tests do not prove it.
There is no `tests/integration/` directory in this repository. Functionaltest,
workersync/controller and affected store checks remain the relevant integration
checks. Mandatory commands, actual exits, base comparisons and remaining gaps
are recorded in the executor report for the final pushed source.
