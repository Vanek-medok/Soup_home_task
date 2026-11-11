# Design Note — Job Queue Service

## 1. Architecture

The service consists of an HTTP API, PostgreSQL, multiple independent workers, and a lease reaper running inside each worker process.

PostgreSQL is the source of truth for job state, ownership, lease expiration, attempts, and committed side effects. Workers coordinate through database operations rather than in-memory locks.

PostgreSQL was chosen because its transactions, row-level locking, partial indexes, and `FOR UPDATE SKIP LOCKED` provide the primitives needed for a reliable concurrent queue without introducing a separate message broker.

## 2. Atomic job claiming

A worker claims a job using a single atomic `UPDATE` statement. A subquery selects the earliest queued job using `FOR UPDATE SKIP LOCKED`, and the outer update changes its state to `running`, assigns a worker ID, sets the lease expiration, and increments the attempt counter.

The row lock prevents competing workers from selecting the same job simultaneously. `SKIP LOCKED` allows other workers to claim different jobs without waiting for already locked rows.

The incremented `attempt` is also used as a fencing token. A worker starts processing only after the claim succeeds.

## 3. Failure detection and recovery

Every running job has a database-managed lease. Workers periodically extend it through heartbeats.

A heartbeat updates the lease only if the job is still `running`, the attempt number matches, and the previous lease has not expired. If no row is updated, the worker has lost ownership or the job has been cancelled, so it stops processing.

Each worker process also runs a reaper. It finds expired running jobs using `FOR UPDATE SKIP LOCKED` and either returns them to `queued` or marks them `failed` when the maximum attempt count has been reached.

Database time is used for lease comparisons, avoiding dependence on worker clock synchronization.

## 4. Stale worker protection

A worker may continue executing after its lease expires, for example, if it is paused and another worker reclaims its job.

To prevent stale completion, `finish()` performs a conditional update requiring the job ID, current attempt number, `running` status, and an unexpired lease to match.

If the update affects zero rows, the transaction is rolled back and no side effect is written.

This fencing mechanism prevents an older attempt from committing after a newer attempt has taken ownership.

## 5. Cancellation and completion races

Cancellation is implemented as a conditional database update that changes only `queued` or `running` jobs to `cancelled`.

Completion also uses a conditional update that requires the job to remain `running` and owned by the current attempt.

Both operations compete for the same database row. Under PostgreSQL's default READ COMMITTED isolation, the update that obtains the row lock and successfully satisfies its condition wins. The competing update rechecks the row after waiting.

If cancellation wins, completion affects zero rows and its transaction writes no effect. If completion wins, cancellation returns the current terminal state with HTTP 409.

Cancellation prevents a later successful commit, but does not necessarily interrupt arbitrary computation instantaneously. The worker's heartbeat detects the state change and cancels its execution context.

## 6. Delivery and side-effect guarantees

The service provides:

- **At-least-once processing attempts:** a job may be executed again after worker failure and lease expiration.
- **At-most-once committed database effect per job:** only a valid current attempt can transition the job to `succeeded` and insert its effect in the same transaction.
- **Not exactly-once execution:** computation may run more than once.

The `effects` table intentionally has no unique constraint on `job_id`. This allows the chaos harness to detect duplicate committed effects instead of silently hiding them through a uniqueness violation.

For external side effects, such as sending emails or calling third-party APIs, the database transaction cannot guarantee exactly-once behavior. Such integrations require external idempotency support or an outbox-based design.

## 7. Idempotency and isolation

Job creation uses a unique constraint on `idempotency_key` and `INSERT ... ON CONFLICT DO NOTHING`. Concurrent requests with the same key resolve to one job. The API returns the existing job if the payload matches and HTTP 409 if the same key is reused with a different payload.

The implementation relies on PostgreSQL's default READ COMMITTED isolation. Row-level locking and conditional updates serialize competing state transitions. The unique idempotency constraint protects concurrent creation requests.

## 8. Trade-offs and limitations

PostgreSQL is both the durable queue and the coordination mechanism. This keeps the architecture simple but makes database availability and transaction behavior critical.

Lease duration and heartbeat frequency trade recovery speed against false reclamation during pauses or slow processing.

The current keyset pagination uses `seq`. Under concurrent inserts, a transaction with a lower sequence value may commit after a reader has advanced beyond it. Therefore, pagination is stable for a quiet dataset but is not a strict snapshot across concurrent inserts.

Before production deployment, the system still needs validation under the target workload, including 10,000 jobs, eight workers, repeated requests, cancellations, process kills, and freezes.