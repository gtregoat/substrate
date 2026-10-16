# ProveEmptyDraining native contract

`ateapi.Control/ProveEmptyDraining` is an authenticated, global-worker-scoped
ownership-evidence boundary. It takes `expected_worker`, the complete Worker
returned by a successful fresh `GetWorker`. The caller must preserve name,
UID, positive version, namespace, pool, Pod name, Pod UID, node name, IP and
sandbox class. It must observe DRAINING. Namespace overrides, partial identity,
stale revisions and name reuse do not waive any precondition.

The current implementation provides concrete native refusals. **It does not grant release eligibility.** In
particular, an actually empty worker with independently retained external data
returns `OWNERSHIP_STABILITY_UNPROVEN`, rather than a success-shaped empty
response. The real useful positive proof requested in the original objective
remains unimplemented. The authorized next correction must retain uncertain
operation origin through crash and later lifecycle transitions before enabling
the clean-worker success path; no additional owner authorization is needed.

The store holds the same PostgreSQL worker row lock used by assignment and
drain, checks SQL/protobuf identity consistency, DRAINING, exact mapping,
allocation, durable assignments, actor pointers and local checkpoints. It reads
raw protobufs without the public read path's deliberate unknown-field discard.
Corrupt/unknown data is uncertainty. No worker, actor, assignment, snapshot,
volume, version or outbox row is changed. An unrelated external snapshot or
historical actor alone is not a node-local dependency. Resume legitimately
retains pause snapshot metadata. Treating it as historical requires a complete
RUNNING assignment, its durable actor-UID claim, and a matching authoritative
worker incarnation/Pod/node observation from the same SQL read. The assignment
must still be on a recorded snapshot-owning node and have no pending snapshot
operation. Missing/malformed pointers, missing claims and inconsistent mappings
remain unknown; RUNNING alone never suppresses snapshot validation. The live
assignment still blocks proof for its own node. This observation does not
settle a failed native operation or establish node-retirement authority.

## Wire outcomes

* `INVALID_ARGUMENT`: malformed input, missing UID or nonpositive version.
* `NOT_FOUND`: worker missing; ownership is unknown.
* `ABORTED`: UID or version changed, including name reuse. Read again.
* `FAILED_PRECONDITION`: native refusal. `google.rpc.ErrorInfo` carries domain
  `ateapi.worker-ownership` and one of the following reasons:

| Reason | Missing or conflicting fact |
| --- | --- |
| `OWNERSHIP_NOT_DRAINING` | Expected/current worker is not DRAINING |
| `OWNERSHIP_MAPPING_CHANGED` | Canonical Pod/node mapping differs or a current target actor pointer contradicts it |
| `OWNERSHIP_BUSY` | Allocation, assignment or relevant actor pointer remains |
| `OWNERSHIP_LOCAL_CHECKPOINT` | A current checkpoint requires the containing node |
| `OWNERSHIP_OPERATION_PENDING` | Relevant recorded upload/checkpoint work remains |
| `OWNERSHIP_UNKNOWN` | Partial, unknown, corrupt or unlocated ownership data |
| `OWNERSHIP_STABILITY_UNPROVEN` | Current observations cannot establish stability through ordinary release |

All errors return no proof response. Transport errors, missing details, unknown
reasons and UNIMPLEMENTED also mean no evidence. Consumers must preserve
protection and must never fall back to DrainWorker, DrainEmptyWorker, list
counts or GetActiveWorkloadStats.

The response schema reserves `worker` and `ownership_generation` for a future
supported success path. Consumers must require a nonempty native ownership
generation and complete exact identity/mapping before treating any future
success as evidence. No generation is minted by this implementation. A lost
response is not permission to release: fresh identity readback and a new proof
attempt are required. GetWorker DRAINING readback alone recovers drain state,
not ownership proof.

## Why the final refusal is necessary

`DrainEmptyWorker` excludes assignment to this worker, but sibling workers can
publish new local state on the containing node after a proof transaction
commits. Actor updates publish checkpoint ownership without the target worker
row lock. Workflow leases lack node dependency provenance after an actor pointer
is cleared. Ordinary Pod cleanup deletes the Worker row, so a worker-row-only
fence would disappear before ordinary node retirement. The source interleaving
and native counterexample tests are recorded in
[worker-retirement-evidence-gap.md](worker-retirement-evidence-gap.md).

Later constraint-conflict steering prohibits new durable schema/fences in this
task. No migration, lease tombstone, retained deleted Worker, controller or
attestation infrastructure is introduced. This refusal path is partial delivery
of the original producer objective, not live cleanup or the complete positive
proof. Root retains review/publication/CI/Kargo deployment, capability rollout,
Goat integration and deployed acceptance. The default-off live guard is unchanged.
