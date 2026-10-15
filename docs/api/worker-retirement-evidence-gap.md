# Worker retirement evidence: unresolved native boundary

Status: investigation only. There is no ownership-evidence RPC in this change.
`DrainEmptyWorker` is not retirement evidence, and a consumer must continue to
refuse release when its ownership authority is unavailable. The existing live
guard remains unchanged.

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

These are outstanding implementation requirements, not an available API or a
claim that the proof exists:

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
by this investigation. Generated API, production producer and new race tests
remain unfinished. Review/publication, CI, Kargo deployment, capability rollout,
Goat integration and deployed acceptance remain owner delivery work; this
source does not authorize or demonstrate live cleanup.
