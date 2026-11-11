# Surprise / Concerns

The most surprising part of this project was how much complexity is hidden behind a seemingly simple job queue. Implementing job submission and worker execution was relatively straightforward. The difficult part was preserving correctness when workers crash, leases expire, heartbeats fail, and cancellation races with job completion.

One important lesson was that a worker successfully finishing its computation does not necessarily mean it still owns the job. This made fencing tokens, conditional database updates, and transaction boundaries essential rather than optional implementation details.

My main concern is the lack of sufficient performance evidence at the target load of 100 RPS. The current chaos tests demonstrate promising recovery behavior, but they do not establish production readiness under 10,000 jobs, sustained load, concurrent cancellations, and worker freezing.

I am also aware that database-level transactions cannot guarantee exactly-once execution of external side effects. Integrations such as payment processing, email delivery, or third-party API calls would require additional idempotency mechanisms or an outbox pattern.

Overall, the project reinforced that building a reliable distributed system is less about implementing the happy path and more about making failure behavior explicit, observable, and safe.