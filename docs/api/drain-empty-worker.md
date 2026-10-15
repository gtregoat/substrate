# DrainEmptyWorker consumer contract

Capability: `/ateapi.Control/DrainEmptyWorker` (use the generated full
method constant for the authoritative protobuf package name). An older server
returns UNIMPLEMENTED. Consumers must stop on that result; never substitute
DrainWorker. This source delivery does not authorize an operational call.

Send `DrainEmptyWorkerRequest` with `worker.name`, empty `worker.atespace`,
`uid` (the UUID from a successful GetWorker) and `version` (its positive int64
revision). Both guards are mandatory. Workers are global resources; the RPC
uses the existing ControlService authentication interceptor and rejects an
atespaced reference. It adds no namespace or administrative bypass.

The server takes the worker row lock used by BindActorToWorker, compares both
guards, checks worker_assignments, allocation and recorded Actor pointers,
then changes ACTIVE to DRAINING. It preserves assignments and Actor state,
including RESUMING. The worker lock serializes assignment/resume admission:
assignment first refuses drain; drain first refuses later scheduler admission.
Legacy DrainWorker retains its name-only behavior.

A successful response is the stored Worker, including UID, resulting version
and DRAINING state. Transitioning increments version once. A fresh matching
request against a DRAINING worker still proves emptiness and returns the
unchanged revision. Concurrent requests using one observed ACTIVE revision
have one winner; stale retries return ABORTED.

- INVALID_ARGUMENT: missing/malformed guards or invalid worker reference.
- UNAUTHENTICATED: rejected by the existing server authentication boundary.
- NOT_FOUND: worker absent; this is not an emptiness observation.
- ABORTED: UID/version mismatch, including name reuse. No write occurred.
- FAILED_PRECONDITION: busy worker or unsupported state. No write occurred.
- UNAVAILABLE, deadline, canceled or transport failure: outcome is unknown.

After an ambiguous response, use an authenticated GetWorker. A successful read
of the same UID in DRAINING reconciles the transition; do not infer current
emptiness from that read alone. If empty-drain proof is needed again, send a
fresh UID/version request to this method, which rechecks emptiness under lock.
A same-UID ACTIVE read can support a fresh guarded attempt. A missing,
unavailable or failed read remains unknown, and a different UID is a different
worker. Never waive guards or send a broad drain to recover a lost response.

Root owns deployed capability verification, final review/CI/release, fresh
candidate safety gates and all live operations. Source-only delivery does not
prove idle cleanup, deployment or operational safety gates completed.
