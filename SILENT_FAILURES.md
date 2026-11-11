# Silent Failures Analysis

A job queue can appear healthy while silently violating its correctness guarantees. HTTP requests may succeed, workers may remain alive, and jobs may eventually reach a terminal state, while duplicate effects, lost work, or inconsistent state transitions remain undetected.

The following failure scenarios are particularly important.

### 1. Lost heartbeat and lease expiration

A worker may continue processing a job while temporarily failing to renew its lease because of a database error or network interruption. Once the lease expires, another worker may reclaim the job.

The original worker may still be executing. Without ownership validation, both workers could commit an effect for the same job.

**Detection and mitigation:**
- Log heartbeat failures and lease-loss events.
- Monitor the number of reclaimed jobs and attempts.
- Require a valid lease and matching fencing token when completing a job.
- Treat a completion affecting zero rows as a rejected stale completion.

### 2. Stale worker completing a reclaimed job

A worker can finish after its lease has expired and another worker has already claimed the job. A successful local computation does not prove that the worker still owns the job.

**Detection and mitigation:**
- Use the monotonically increasing `attempt` value as a fencing token.
- Make completion conditional on job ID, attempt, running status, and an unexpired lease.
- Record rejected completions and compare attempts in the event log.

### 3. Cancellation and completion race

Cancellation may arrive while a worker is finishing a job. Without conditional state transitions, one operation could overwrite the result of the other.

**Detection and mitigation:**
- Use conditional SQL updates for both cancellation and completion.
- Rely on PostgreSQL row locking and transaction semantics to serialize competing updates.
- Verify that cancellation produces no committed effect and that a completed job cannot subsequently become cancelled.
- Return a conflict response when cancellation loses the race.

### 4. Duplicate job submissions

Clients may retry requests after timeouts or lost responses. Without idempotency, the same logical operation could create multiple jobs.

**Detection and mitigation:**
- Enforce a unique constraint on `idempotency_key`.
- Compare the payload when a key already exists.
- Return the existing job for an identical request and `409 Conflict` when the same key is reused with a different payload.
- Test concurrent submissions using the same key.

### 5. Partial completion and inconsistent effects

A worker could update a job's status successfully but fail while recording its business effect. If these operations are committed separately, the database may contain an inconsistent result.

**Detection and mitigation:**
- Update the job status and insert the corresponding effect in the same database transaction.
- Roll back the transaction if effect insertion fails.
- Track the number of effects and unique affected job IDs.
- Investigate any mismatch between succeeded jobs and committed effects.

### Limitations

Database-level fencing and transactions protect database effects, but they do not guarantee exactly-once execution of arbitrary external operations. An external API call, email, or payment may be repeated after a worker crash. Such operations require their own idempotency mechanism or an outbox-based integration.

A successful health check alone is also insufficient to prove queue correctness. Correctness must be verified using final database state, attempt counts, effect records, event logs, and chaos-test results.