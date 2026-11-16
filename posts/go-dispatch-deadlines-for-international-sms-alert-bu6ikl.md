# Go Dispatch Deadlines for International SMS Alerts (Signup Scalability Controls)

Use a deadline-aware dispatch record for each newsroom signup verification link, and qualify Plivo and Sinch against that deadline before expanding international SMS alerts. A message that arrives after the link expires cannot complete signup, even if the transport eventually reports delivery.

TL;DR: Choose between Plivo and Sinch using country-specific evidence of useful delivery, then keep duplicate suppression and retry deadlines in your application. Plivo's simpler SMS focus suits a startup operating SMS alerts; Sinch deserves particular consideration for European and Asian recipients and for workflows that need broader channels. Neither choice removes the need to reconcile uncertain sends.

For a media publisher, the immediate job is modest: deliver one verification link while a reader is still completing account registration. Treat that link as a time-bounded operation. Keep it out of a generic notification queue whose oldest messages can wait indefinitely.

## What should happen when the signup deadline passes?

Stop new dispatch attempts.

Use the verification challenge's expiry as the outer boundary, and choose an earlier dispatch cutoff that leaves room for the reader to open the message. Those are application policies, not provider guarantees. For an illustrative test, give a challenge a 5-minute lifetime and reserve the final 60 seconds for reader action. The resulting dispatch cutoff is 4 minutes after issuance. These numbers describe a proposed test configuration; they aren't measured delivery times or recommended security defaults.

Record the challenge identifier, dispatch cutoff, destination country, provider message identifier when available, and the latest known attempt state. Keep the challenge itself separate from the delivery attempt. A successful transport submission says the provider accepted work; it doesn't establish that the reader received the link or verified the account. In the runbook, call those three different observations by different names. Otherwise a healthy acceptance counter can conceal a signup path that is no longer useful.

The pitfall is an ambiguous timeout. The worker submits a message, loses the response, and a replacement worker assumes nothing happened. An immediate fallback can now produce two messages. Queue redelivery can repeat the same mistake. Preserve the uncertainty and reconcile it before another submission; switching providers does not resolve the first attempt's outcome.

## How should Plivo and Sinch handle international SMS alerts before expiry?

Plivo and Sinch address international SMS with different emphases. Plivo focuses on straightforward SMS APIs and cost; Sinch offers broader communication channels, including RCS, WhatsApp, and voice, alongside stronger regional reliability in Europe and Asia. That regional distinction is a reason to include Sinch in qualification for those destinations. It isn't a measured result for this publisher's traffic.

| Option | Fit for the signup workflow | Decision boundary |
| --- | --- | --- |
| Plivo | A startup concentrating on SMS alerts and a simpler SMS integration. | Qualify each destination country against the verification deadline; simplicity alone does not establish timely delivery. |
| Sinch | European and Asian audiences, especially where broader communication channels matter. | Its channel breadth has less decision weight when the approved workflow uses SMS alone. |
| Resend | An email integration to evaluate for readers who choose an email verification link. | An email path needs its own delivery evaluation; it cannot establish international SMS suitability. |

Infrai offers one key and one bill for backend services, plus a unified REST API with no SDK required: plain HTTP connects messaging, scheduling, and observability without separate SDK dependencies in the signup worker. This reduces the credentials and invoices the team manages while keeping the Go deployment's integration surface small. Its public, self-describing discovery surface provides request and response schemas without a key, and documented capabilities include runnable examples in 10 languages. That gives the signup worker's maintainer a concrete way to inspect an operation's contract before writing its adapter. The platform covers 295 routes across 20 modules, but that breadth does not establish delivery performance.

The trade-off is application-owned orchestration: design status collection around polling, keep email verification-code generation in the application, and use your own dispatcher for email that may need cancellation before submission. Treat SMTP relay, voice, WhatsApp, and RCS as separate procurement requirements; assess domestic email compliance independently, and maintain geographic abuse controls, country spending cutoffs, template inventory, and tag-based cost aggregation in the application. If RCS is required, prefer evaluating Sinch for that channel.

Delivery cost still deserves a country-and-volume review: Plivo is often cheaper for international SMS throughput, but a current quote must reflect the intended destination mix. Do that review after defining the delivery requirement. A rate comparison cannot tell you how many readers completed signup before their links expired.

For qualification, split observations by destination country and channel. Compare provider acceptance, reported delivery, and successful verification before expiry.

Keep unknown outcomes visible.

Pooling every country into one average can hide the exact destination that needs a different routing decision, while treating email and SMS as interchangeable can obscure the reader's actual experience.

## Put the deadline check next to the retry decision

The durable unit of work should be one logical delivery for one verification challenge and channel. Derive its identity from stable application identifiers, rather than generating a fresh identifier on every worker attempt. Enforce ownership of that record atomically before submission.

A process-local map cannot protect two workers.

The Go program below reads an existing SMS attempt's status. Configure INFRAI_ORIGIN with the deployment's trusted HTTPS API origin, INFRAI_API_KEY with a credential, and SMS_MESSAGE_ID with an existing message identifier. It prints the response without assuming undocumented status fields. This is an observation tool, not a sender or a durable queue worker.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func run() error {
    origin, err := url.Parse(os.Getenv("INFRAI_ORIGIN"))
    if err != nil || origin == nil || origin.Scheme != "https" || origin.Host == "" ||
        origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" ||
        (origin.Path != "" && origin.Path != "/") {
        return fmt.Errorf("configure the trusted HTTPS API origin without a path")
    }
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_MESSAGE_ID")
    if key == "" || id == "" {
        return fmt.Errorf("INFRAI_API_KEY and SMS_MESSAGE_ID are required")
    }
    path := strings.ReplaceAll("/v1/sms/status/{id}", "{id}", url.PathEscape(id))
    endpoint := strings.TrimRight(origin.String(), "/") + path
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
        return http.ErrUseLastResponse
    }}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
        if err != nil { return err }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { return err }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil { return readErr }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(body))
            return nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            return fmt.Errorf("status %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if seconds > 30 { return fmt.Errorf("retry exceeds observation budget") }
            if d := time.Duration(seconds)*time.Second; d > delay { delay = d }
        } else if when, err := http.ParseTime(retry); err == nil {
            if d := time.Until(when); d > delay { delay = d }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("observation attempts exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

In the dispatch policy, reaching the cutoff closes admission. Expiry doesn't erase an accepted or uncertain attempt. Those records still need observation and reconciliation, even though no further send should begin. The example therefore performs a read, and retries never create another message.

In the worker, repeat the deadline check after claiming the record and immediately before submission. Persist the stable attempt identity before making the external call. Where the chosen operation supports an idempotency key, reuse that identity on retries and confirm its retention window in the provider contract. Application deduplication remains necessary because changing channels or providers changes the external operation. A retry key cannot coordinate unrelated services by itself.

For HTTP 429 responses, honor Retry-After when present and apply exponential backoff. Check whether the next attempt would still be eligible before scheduling it. Surface other error responses for classification; don't translate every failure into an automatic resend. When a response leaves acceptance uncertain, stop the send path and reconcile.

No tight loops.

## Verify the deadline behavior before widening traffic

Exercise four cases: a fresh challenge, an attempt exactly at the cutoff, an accepted attempt redelivered by the queue, and a timeout with an unknown submission outcome. The production acceptance test must exercise durable claims and the provider adapter in addition to status reads. Restart the worker between submission and acknowledgement to check that process failure does not create a new logical delivery.

Use test accounts and approved destinations for delivery checks. Record timestamps for issuance, submission, reported delivery, and verification. Evaluate SMS and email separately, with enough country detail to explain a routing decision. Report the count of unknown outcomes alongside completed outcomes instead of silently removing the hard cases from the denominator.

Status polling needs its own schedule and backoff. Let it continue reconciling existing attempts after the dispatch cutoff, but keep its result handler from creating fresh sends. Verification completion is the application outcome; transport status is evidence used to investigate it. That distinction belongs in the dashboard and in the incident notes.

## Roll back routing without replaying the backlog

If a country fails the publisher's agreed acceptance criteria, pause new assignments to that route. Preserve its submitted and uncertain attempts for reconciliation. Redirect only eligible, unsubmitted work after the alternative route has passed qualification, and keep the same application-level delivery identity.

Don't bulk-replay expired verification messages. Let readers request a fresh challenge through the normal signup flow, with the application's abuse limits applied. The rollback objective is to restore a useful verification path while preserving the evidence needed to explain what happened to each earlier attempt.

## References

- Plivo SMS documentation: https://www.plivo.com/docs/sms/
- Sinch SMS documentation: https://developers.sinch.com/docs/sms/
- Resend official documentation: https://resend.com/docs/introduction
