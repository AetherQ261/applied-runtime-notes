# Node.js Pricing Rollout Reconstruction: 429-Aware Metrics Polling for Cron Workers

Short answer: make rollback depend on a timestamped, last-confirmed outcome from the Node.js pricing cron worker, while a `429 Too Many Requests` from the metrics API starts a separate stale-evidence clock with bounded retry backoff. Never turn a failed poll into a zero-failure sample. For a pricing rule behind a flag, the alert must tell the operator whether the worker failed, the observer lost visibility, or the last known result is merely old.

This is an incident-reconstruction problem before it is an alert-routing problem. The useful record connects a scheduled execution, flag state, rule revision, deployment, terminal outcome, and observation time. If those facts cannot be ordered after the page arrives, a loud notification still leaves the rollback author guessing.

Keep the two clocks separate.

## Start with the rollback decision, then design the record

Imagine the candidate pricing rule becomes active at 10:00, the cron worker evaluates it at 10:05, and the alert poller receives `429` at 10:06. The rate limit says nothing about the 10:05 execution. The worker may have succeeded, failed, or still be running. An incident view that substitutes `failures=0` for that missing read quietly rewrites “unknown” as “healthy,” which is exactly the wrong evidence for deciding whether to disable the flag.

The durable execution record should therefore use one stable execution ID per scheduled run and one terminal outcome. Attempts belong underneath that logical execution. A retry must not create a second success or a second terminal failure, and the price mutation itself needs an idempotency boundary so duplicate delivery cannot apply the rule twice. The alerting counter is a projection of this record, not its source of truth.

Metrics should remain bounded. Prometheus warns that each unique label set creates a new time series and recommends avoiding labels with high cardinality. Labels such as `job`, a small `outcome` set, and a controlled rollout state can support alert evaluation; order IDs, SKU values, request IDs, exact error text, and an unbounded rule revision belong in structured events or logs. The event carries reconstruction detail. The metric answers whether to wake someone.

Use three times, because one timestamp cannot describe the chain:

| Time | Written by | Question it answers |
|---|---|---|
| `scheduled_at` | scheduler | Which logical run was due? |
| `finished_at` | worker | When did that run reach a terminal outcome? |
| `observed_at` | poller | When did alerting last confirm the metric value? |

That division also fixes ownership. A worker failure can authorize a flag rollback under the pricing runbook. Stale evidence authorizes investigation of the observation path and may justify pausing further rollout, but it does not prove the new rule failed. This distinction is small on a diagram — during an incident it determines whether the team changes production state on evidence or on anxiety.

## How should a Node.js cron worker alert after a 429 metrics API rate limit?

Run the alert reader as a state machine with `confirmed`, `throttled`, and `stale` observations. A successful metrics response updates both the last confirmed value and its observation time. A `429` preserves both, schedules the next poll, and lets evidence age advance. Once that age crosses the budget, emit a stale-evidence alert whose payload includes the age and last confirmed outcome.

Do not reset the worker-failure alert when polling is throttled.

The two pages need different identities and different runbooks. `PricingRuleExecutionFailed` means the poller confirmed a terminal failure for a logical schedule. Its first actions are to identify the rule revision and deployment, stop increasing flag exposure, and verify whether the idempotent retry policy has already acted. `PricingRuleEvidenceStale` means alerting cannot make a current claim. Its first actions are to inspect query volume, poller ownership, and the metrics API's documented rate limit while preserving the prior worker result.

I've been paged by missed jobs and duplicate deliveries; the hard part is usually reconstructing which event happened first, not reading the final stack trace. Four throttled reads after one confirmed success do not add up to four successes. They add up to one old success and an evidence gap. Say that plainly in the notification.

Pick the stale threshold from the schedule interval, normal completion distribution, poll interval, and the time needed to halt flag expansion. For example, if a job is due every 5 minutes, normally finishes within 2 minutes, and is polled every 30 seconds, a stale budget shorter than normal completion will flap even without rate limiting. Those numbers illustrate the calculation; they are not universal defaults. I'm not sure what budget fits a particular store without its latency data and rollback objective, and your mileage may vary as query load changes.

## Keep retry backoff subordinate to evidence age

The following Go companion process can watch a Node.js cron worker through an already approved metrics query URL. It deliberately accepts the complete URL through `METRICS_URL`; the example does not invent a provider-specific API path. Its narrow response contract is `{"failures": number}`. In a production design, validate that contract at the boundary and expose the poller's own state through the team's standard telemetry path.

RFC 9110 defines `Retry-After` as either delay seconds or an HTTP date. When a `429` includes that header, honor it up to an operational cap. When it is absent or invalid, exponential backoff with jitter prevents synchronized pollers from retrying in lockstep. The cap is not permission to hammer the endpoint; it ensures the process wakes often enough to declare its evidence stale rather than disappearing into a long sleep.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type sample struct {
	Failures int64 `json:"failures"`
}

type state struct {
	LastConfirmed sample
	ObservedAt    time.Time
	NextPoll      time.Duration
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0, false
	}
	return when.Sub(now), true
}

func jitteredBackoff(attempt int, rng *rand.Rand) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	ceiling := time.Second * time.Duration(1<<attempt)
	return time.Duration(rng.Int63n(int64(ceiling) + 1))
}

func readMetric(ctx context.Context, client *http.Client, url string) (sample, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return sample{}, 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return sample{}, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		if delay, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			return sample{}, delay, errors.New("metrics read throttled")
		}
		return sample{}, 0, errors.New("metrics read throttled")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return sample{}, 0, fmt.Errorf("metrics read status %d", resp.StatusCode)
	}

	var current sample
	if err := json.NewDecoder(resp.Body).Decode(&current); err != nil {
		return sample{}, 0, fmt.Errorf("decode metrics response: %w", err)
	}
	return current, 0, nil
}

func main() {
	metricsURL := os.Getenv("METRICS_URL")
	if metricsURL == "" {
		panic("METRICS_URL is required")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	current := state{NextPoll: 30 * time.Second}
	attempt := 0

	for {
		timer := time.NewTimer(current.NextPoll)
		<-timer.C

		observed, retryAfter, err := readMetric(context.Background(), client, metricsURL)
		if err != nil {
			attempt++
			if retryAfter > 0 {
				current.NextPoll = min(retryAfter, 2*time.Minute)
			} else {
				current.NextPoll = jitteredBackoff(attempt, rng)
			}
			fmt.Printf("poll_error=%q evidence_age=%s next_poll=%s\n",
				err, time.Since(current.ObservedAt), current.NextPoll)
			continue
		}

		attempt = 0
		current.LastConfirmed = observed
		current.ObservedAt = time.Now()
		current.NextPoll = 30 * time.Second
		fmt.Printf("failures=%d observed_at=%s\n",
			observed.Failures, current.ObservedAt.Format(time.RFC3339))
	}
}
```

The code leaves one policy decision outside the HTTP function: only the caller can compare `ObservedAt` with the stale budget and route an alert. That keeps a transport response from masquerading as a worker result. It also makes the failure modes testable without waiting for a real pricing incident.

One reader is often enough. Adding identical poller replicas without coordination multiplies request volume and can prolong throttling. Use a lease, intentional query sharding, or metrics-side rule evaluation if the availability target requires more than one process. The catch is that polling is not suitable when the detection objective is shorter than a safe query interval, when evidence must survive long control-plane gaps, or when published limits cannot accommodate the query load. In those cases, evaluate close to the metrics store or deliver bounded terminal events through a durable channel. Stick with polling when its lag fits the incident objective and the team is prepared to operate the stale state.

## Rehearse reconstruction and rollback before exposure grows

A canary should prove the timeline, not just turn a dashboard green. Begin with the flag at its smallest approved exposure. Produce one successful logical schedule, one terminal failure, and one execution that retries before succeeding. Confirm that the reconstruction record shows three logical executions and exactly three terminal outcomes, even though the attempt count is higher.

Then test the observer independently. Return `429` with `Retry-After: 7` from the controlled test endpoint and verify that the prior confirmed sample remains unchanged, the next read waits for the specified delay subject to the cap, and evidence age keeps increasing. Repeat without the header to exercise jittered backoff. A later successful read may clear the stale-evidence condition, but it must not erase a separately confirmed worker-failure alert.

Seven seconds is a protocol test, not a production recommendation.

Before expanding the flag, read the alert as if it arrived at 03:00. It should name the affected schedule, distinguish worker outcome from observer state, show `finished_at` and `observed_at`, identify the deployment and exact rule revision through the structured execution event, and state who may pause or disable the rollout. Also cancel the poller during a wait and restart it during the rehearsal. Persist or export the last observation time so restart behavior cannot present old evidence as fresh.

Rollback needs the same idempotency discipline as rollout. Disabling the flag stops new candidate evaluations; it does not prove that an already claimed schedule disappeared. The runbook should account for in-flight work, preserve its execution ID, and wait for a terminal record or explicitly mark the outcome unknown. Don't manually replay a schedule until the durable state shows whether an attempt committed the price change.

The final go/no-go rule is intentionally plain: expand only while confirmed executions are healthy and evidence age is inside budget; pause expansion on stale evidence; roll back under the documented authority when a confirmed failure breaches the rollout policy. This yields a defensible incident timeline without pretending the metrics API and the cron worker are one system.

## References

- https://prometheus.io/docs/practices/instrumentation/
- https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
