# Design Note — Job Queue Service

## 1. Architecture

The service consists of an HTTP API, PostgreSQL, multiple workers, and a lease reaper.

The API handles job creation, status retrieval, pagination, and cancellation. PostgreSQL is the source of truth for job state, worker ownership, lease expiration, and committed side effects. Workers coordinate through the database rather than in-memory locks.

PostgreSQL was chosen because it provides transactions, row-level locking, constraints, and `FOR UPDATE SKIP LOCKED`, allowing concurrent job processing without an additional message broker.

## 2. Atomic job claiming

A worker claims a job using a single atomic SQL statement. A subquery selects the next queued job using `SELECT ... FOR UPDATE SKIP LOCKED`; the outer `UPDATE` changes its status to `running`, increments its attempt number, assigns the worker ID, and sets the lease expiration time.

`SKIP LOCKED` allows concurrent workers to skip rows already locked by other workers instead of waiting for them. The status condition in the outer update provides an additional safeguard.

The worker starts execution only after the claim statement succeeds.

## 3. Leases, heartbeats, and recovery

Every running job has a lease represented by `lease_expires_at`. The worker periodically sends heartbeats to extend the lease.

A heartbeat is accepted only if the job is still running, the attempt number matches, and the previous lease has not expired. If the update affects zero rows, the worker stops its local execution because it has lost ownership or the job has been cancelled.

Temporary database errors during heartbeat do not immediately abort execution. The worker retries, while the final completion operation independently checks ownership and lease validity.

Each worker also runs a reaper. It finds running jobs whose leases have expired and either returns them to `queued` or marks them `failed` when the maximum attempt count has been reached.

Lease timestamps are calculated using PostgreSQL's clock, rather than individual worker clocks. This avoids relying on synchronized clocks across worker machines.

## 4. Fencing and stale workers

Lease expiration alone cannot prevent an old worker from continuing after a pause or network interruption.

Every successful claim increments the job's `attempt` value. This value acts as a fencing token. Heartbeats and completion operations must match the current attempt number.

When a job is reclaimed, its attempt number changes. A stale worker can no longer complete the job because its conditional update affects zero rows.

The completion operation uses a database transaction:

1. Conditionally update the job to `succeeded`, checking its ID, current attempt, running state, and unexpired lease.
2. If no row was updated, reject the stale completion.
3. Insert the side-effect record.
4. Commit both changes together.

If inserting the effect fails, the transaction rolls back, including the status update. This prevents a successful job state without its corresponding committed database effect.

This fencing mechanism protects database writes. It does not automatically prevent duplicate external actions if future job handlers call third-party services. Such integrations need external idempotency support or an outbox-based design.

## 5. Cancellation and completion races

Cancellation and completion are coordinated through conditional updates on the same job row.

Cancellation changes the state only when it is `queued` or `running`. Completion changes the state only when it is `running` and the attempt and lease checks succeed.

Under PostgreSQL's default `READ COMMITTED` isolation, concurrent updates to the same row are serialized by row-level locking. After waiting for a competing update, PostgreSQL rechecks the update condition.

Therefore:

- If cancellation wins, the job becomes `cancelled`; completion no longer matches `status = 'running'` and cannot insert an effect.
- If completion wins, the job becomes `succeeded` and its effect is committed; cancellation finds a terminal state and returns HTTP 409.

Cancellation prevents a later successful database commit, but it does not necessarily stop arbitrary computation immediately. The worker notices lost ownership through its heartbeat.

## 6. Delivery and side-effect guarantees

The system provides:

- **At-least-once processing attempts:** a job can be executed again after a worker failure and lease expiration.
- **At-most-once committed database side effects per job:** only the current running attempt can transition the job to `succeeded` and insert its effect in the same transaction.
- **Not exactly-once execution:** computation may happen more than once, even though only one database effect is committed.

For external side effects, a database transaction cannot guarantee exactly-once behavior across independent systems. External idempotency or an outbox-based integration is required.

## 7. Idempotency and pagination

Job creation uses a unique idempotency key and `INSERT ... ON CONFLICT DO NOTHING`. Repeated requests with the same key and payload return the existing job. Reusing the key with a different payload returns HTTP 409.

Listing uses keyset pagination based on the job sequence number (`seq`) instead of `OFFSET`. This is efficient and stable for ordinary pagination, but there is a known limitation: sequence values are allocated before transaction commit. Under concurrent inserts, a transaction with a smaller sequence value may commit after a reader has already passed that cursor. That job can therefore be missed during that particular traversal.

## 8. Shutdown behavior and trade-offs

On SIGTERM, a worker stops claiming new jobs and attempts to finish its current job. On SIGKILL, it cannot clean up; the lease eventually expires and the reaper recovers the job.

The database is both the durable job store and the coordination mechanism. This keeps the architecture relatively simple, but makes PostgreSQL availability and transaction behavior critical.

Lease duration and heartbeat frequency involve a trade-off: long leases delay recovery, while short leases increase the chance of reclaiming jobs during temporary pauses or slow processing.

Before production deployment, the system needs validation under the target workload, including worker termination, process freezes, duplicate requests, cancellation races, and recovery after lease expiration.