# Daily Report Email Reliability: Idempotent Retries After Cron Admission

Short answer: let the daily cron run admit durable email jobs, then let workers perform delivery with a stable idempotency key and bounded retries. Sending every message inside the cron handler makes one timeout, rate limit, or process restart look like a failure of the whole report.

The queue is only part of the design. The useful contract is durable admission, at-least-once execution, and a delivery ledger that makes repeated attempts harmless where the mail interface permits it.

## The incident lesson: the scheduler was doing two jobs

I've been paged for missed reports and duplicate deliveries. The recurring failure pattern was simple: a scheduled process both decided who should receive a report and held open every outbound request. One slow recipient consumed the remaining time budget. A restart replayed an uncertain slice of the list. The next cron invocation could overlap before the first one had left a useful record of progress.

That postmortem gave me a boundary I still use in reviews: a schedule should create work, not perform unbounded network work. The handler computes the report date, reads a consistent audience snapshot, and records one logical delivery per recipient. Its success condition is committed work. It is not “all remote mail calls returned.”

Short paragraph. Keep this boundary visible in the runbook.

The practical sequence is worth spelling out because each step closes a different failure window. At the scheduled time, the handler takes one snapshot of the audience and report inputs, derives a key for each recipient, and inserts those keys in one transaction. A uniqueness constraint turns a second invocation into a no-op for already admitted work. The transaction also writes an outbox event or queue record; publishing before the insert commits creates a job that has no durable owner, while inserting without an outbox leaves work that no worker can see. A worker leases one record, increments its attempt count, and applies a concurrency limit before calling the mail service. If the call is slow, the lease expires and another worker can claim it. If the service accepts the message and the process dies before the receipt is stored, the next attempt reuses the same business key when the provider supports idempotency. If it does not, reconciliation must mark the delivery as uncertain for review. None of these steps makes the network exactly once; together they keep a single uncertain recipient from replaying an entire morning's batch.

The alternative is a single cron loop with a larger timeout. That can be reasonable for a tiny, low-consequence list, but it couples retry scope to process lifetime. A queue or database-backed work table lets a worker retry one delivery while other recipients continue, and a concurrency limit gives the mail endpoint room to recover. HTTP 429 means the caller exceeded a rate limit; a response may carry `Retry-After`, which the worker should use when choosing its next attempt (see the MDN reference below).

## What makes daily email retries safe after cron scheduling?

Start with identity, not with an attempt number. For a daily report, a key such as report date, recipient ID, and template version names the business event. Store it under a uniqueness constraint. If the scheduler runs twice, the second run observes the existing delivery instead of inventing a second one. Publish a queue reference only after the transaction that stores the delivery commits; a transactional outbox is one way to preserve that handoff.

Workers claim a pending record with a lease. They record attempts, send using the same key when the downstream API supports idempotency, and mark the record complete only after a receipt is durable. Transient failures go to bounded exponential backoff with jitter. A valid `Retry-After` takes precedence for a rate-limited response. Invalid addresses and policy rejections are terminal; exhausted transient attempts belong in a dead-letter queue for inspection and deliberate redrive, a pattern described in the SQS documentation.

```go
package delivery

import (
	"context"
	"fmt"
	"time"
)

type Job struct {
	ReportDate      time.Time
	RecipientID     string
	TemplateVersion string
	Address         string
}

type Ledger interface {
	Claim(context.Context, string, time.Duration) (bool, error)
	Complete(context.Context, string, string) error
	RetryAt(context.Context, string, time.Time) error
}

type Mailer interface {
	Send(context.Context, string, string) (string, error)
}

func DeliveryKey(job Job) string {
	day := job.ReportDate.UTC().Format("2006-01-02")
	return fmt.Sprintf("daily-report:%s:%s:%s", day, job.RecipientID, job.TemplateVersion)
}

func Deliver(ctx context.Context, ledger Ledger, mailer Mailer, job Job) error {
	key := DeliveryKey(job)
	claimed, err := ledger.Claim(ctx, key, 2*time.Minute)
	if err != nil || !claimed {
		return err
	}

	receipt, err := mailer.Send(ctx, job.Address, key)
	if err != nil {
		return ledger.RetryAt(ctx, key, time.Now().Add(30*time.Second))
	}
	return ledger.Complete(ctx, key, receipt)
}
```

There is an unavoidable ambiguity if a provider accepts the message and the worker crashes before `Complete`, especially when the provider has no idempotency contract. The ledger reduces duplicate work; it cannot promise exactly-once delivery across two independent systems. Reconciliation should surface that uncertainty instead of hiding it.

## Compare failure boundaries before choosing the machinery

I compare designs by what gets replayed, where pressure accumulates, and what evidence survives a bad morning.

| Design | Retry unit | Pressure control | Duplicate boundary | Appropriate use |
| --- | --- | --- | --- | --- |
| Cron sends directly | Whole run or an ad hoc slice | Process limits and timeouts | Loop position and local state | Tiny list, low consequence, rerun is acceptable |
| One job per recipient | One logical delivery | Worker concurrency and queue depth | Stable key plus ledger | Independent recipients and variable latency |
| Bounded chunks | One chunk, with checkpoints | Chunk size plus worker limits | Per-recipient keys still required | Very large audiences where queue overhead matters |
| Workflow coordinator | A modeled step | Coordinator and worker limits | Explicit workflow state | Ordered preparation, approvals, or a shared cutoff |

Per-recipient jobs provide the cleanest audit trail, but they add queue operations, state rows, retention policy, and dead-letter ownership. Chunks reduce that overhead while increasing replay scope. A workflow makes barriers explicit while adding another system to deploy and observe. Those are engineering costs, not reasons to pick a particular vendor.

Payloads should carry stable IDs and a schema version rather than a silently stale rendered email. Generate content from a report snapshot, and retain enough provenance to answer which data and template produced a message. During deployment, keep old decoders until the queue retention window has passed; otherwise a routine rollout becomes a data migration incident.

## Operate retries as a state machine, not a timer loop

Useful states are `pending`, `leased`, `retry_wait`, `sent`, and `terminal`. Transitions must be conditional on the current lease owner, and an expired lease must make the job eligible again. Tests should run the same scheduler input twice and assert one logical key; deliver the same message twice and assert one completed ledger entry; then kill a worker after claim and verify lease takeover.

Metrics should answer customer questions. Track schedule admission, oldest pending age, queue depth, lease expirations, attempts, terminal outcomes, and completion lag by report date. An alert that cron ran is weak. An alert that the oldest unfinished delivery exceeds the report objective points at impact.

I've found synthetic jobs useful during a canary, provided they cannot reach customer addresses. Inject 429 responses with and without `Retry-After`, advance a fake clock, and assert delayed retries plus a lower concurrency ceiling. I'm not sure any universal backoff value exists; derive it from the mail service quota and observed latency, then write the choice into the runbook with an owner and review date.

## When is a queue the wrong trade-off for cron email delivery?

The catch is operational weight. A queue is not suitable when the audience is genuinely tiny, the message is low consequence, the completion window is generous, and rerunning a deterministic batch is acceptable. Stick with a bounded cron handler in that case: use a stable run key, prevent overlap, persist per-recipient outcomes, and enforce a hard deadline.

Use a workflow or batch coordinator when the report needs an ordered data freeze, approval, an all-recipient cutoff, or one final aggregate artifact. Use chunks when queue-operation volume dominates and the team can implement per-recipient checkpoints inside each chunk. Every option still needs a definition of retryable versus terminal errors and a person who owns the evidence.

My decision rule is narrow: queue independent work when remote latency varies, individual retries matter, and the team will inspect dead letters. If the last condition is false, the queue only moves the failure out of sight. The defensible target is durable admission, at-least-once execution, idempotent effects where supported, and reconciliation for the remaining ambiguity.

## Further reading

- AWS, “Amazon SQS dead-letter queues”: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- MDN, “429 Too Many Requests”: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
