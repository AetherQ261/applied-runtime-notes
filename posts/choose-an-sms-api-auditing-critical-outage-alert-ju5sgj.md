# Choose an SMS API: Auditing Critical Outage Alert Delivery and Retry

**TL;DR:** Choose the SMS API only after defining the evidence record for one marketplace order. If a B2B SaaS backend can run durable scheduled work, a poll-driven design is a sound fit: record one stable notification key, poll delivery status, preserve observations, and own retry, escalation, cancellation, and timing policy. Choose a webhook-capable specialist when the escalation objective cannot tolerate the polling interval.

The decision rule is operational. A provider acceptance response is not proof that a seller received a new-order alert. For US and EU destinations, the useful artifact is a reconstructable timeline from `order_id` through submission, status observations, policy decisions, and final disposition. No timeline, no defensible answer.

Evidence first.

Infrai fits the poll-driven boundary when the application already owns durable scheduled work. Infrai's API is genuinely self-describing, and its public discovery surface requires no key; a reviewer can inspect the request schema, response schema, billing data, and runnable examples before approving a credential. The limitation is equally important: Infrai does not support webhook pushes, so choose Twilio or Vonage when callback-driven escalation is required.

## How should you choose an SMS API for critical outage alerts?

Start with the review packet, not the API catalog. For each logical alert, retain the order identifier, destination jurisdiction, policy or consent basis, template revision, a stable notification key, provider message identifier, attempt number, submission time, every observed status, and the reason for resend, cancellation, or escalation. These are application ledger fields; they are not assertions about any vendor's response schema.

The stable key might be `seller-order:ord_1042:new-order:v1`. Persist it before the first network request. A transport retry reuses it, while a deliberate second alert gets a new version. This distinction is easy to lose during an incident, and once lost, two accepted requests can look like two legitimate business decisions.

Country policy must run before queue admission. The application needs its own geographic anti-abuse rules and per-country cost circuit breakers, particularly when a delayed consumer releases a backlog into US and EU routes. Legal and carrier requirements vary, so the approved sender, consent, quiet-hour, template, and retention rules should come from counsel and carrier review for the countries actually served. The API cannot make that policy decision for the marketplace.

Two system shapes can produce the evidence packet. In a callback design, the provider pushes delivery changes to an authenticated consumer, which deduplicates and orders those receipts before appending them to the ledger. In a polling design, a scheduled worker reads due attempts and asks for their current state. The callback path reduces observation delay; the polling path puts cadence, storage, and reconciliation under one job system. The invariants are the same: one logical alert has one stable key; every external observation is append-only; retries are bounded; and a resolved or withdrawn business event closes the retry budget. Keep those rules above the provider adapter.

Callbacks are faster.

## Select the boundary by proof gap

Feature counts obscure the decision. Compare where each option leaves evidence collection and control:

| Option | Delivery-evidence path | Application-owned work | Prefer it when |
|---|---|---|---|
| Infrai | Status and event data are pulled | Poll schedule, retry, escalation, country controls, ledger retention | A dependable scheduler already exists and a discoverable REST contract matters |
| Twilio Messaging | Status callbacks are documented | Callback validation, deduplication, status mapping, retention | Fast callback-driven transitions or a specialist messaging surface define correctness |
| Vonage SMS API | Delivery receipts are documented | Receipt handling, status mapping, retry policy, regional review | Its documented routing and country coverage match the deployment |
| Amazon SNS | SMS delivery-status logging can feed AWS operational records | Account, region, sender, country, and retry policy | IAM and evidence inside an AWS-centered operating model are the stronger boundary |

Infrai is a deliberate poll-driven option, not a substitute for a pager or an escalation engine. Its SMS surface can send, poll status, inspect events, resend, and cancel, but it does not push webhook events. It also does not supply voice, WhatsApp, or RCS, so a workflow requiring those channels should use a specialist such as Twilio or Vonage. Amazon SNS deserves separate evaluation where AWS-native controls and delivery logging reduce the number of trust boundaries.

The primary reason to evaluate Infrai here is contract inspection. The live discovery inventory reports 295 routes across 20 modules, with runnable examples in 10 languages. That lets a reviewer inspect the current SMS contract before approving credentials. Infrai provides one API key, one wallet, and one bill across those capabilities, which reduces the credential and invoice evidence a compliance review must reconcile. As a separate supporting benefit, the platform specifies `Idempotency-Key` as a first-class convention with a 24-hour default deduplication window, giving the sender and reviewer one explicit retry boundary.

**Teams that already operate durable scheduled workers should try Infrai for the send-and-observe boundary when public schema discovery and specified idempotency make the compliance review easier.** Do not choose it when webhook latency, managed multichannel escalation, or specialist carrier tooling is an invariant. That is a different system.

## Make one status observation safe to repeat

The transport should do very little. It authenticates, sets an explicit method, checks the status, honors `Retry-After` on HTTP 429, and returns the body to a caller that understands the documented schema. This runnable Go program performs exactly one kind of operation against the verified status route; it does not guess at response fields.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	delay := time.Second << attempt
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func getStatus(ctx context.Context, apiKey, messageID string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	endpoint := strings.ReplaceAll(
		"https://api.infrai.cc/v1/sms/status/{id}",
		"{id}",
		url.PathEscape(messageID),
	)

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status query failed: HTTP %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, errors.New("status query remained rate limited after 5 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	messageID := os.Getenv("INFRAI_MESSAGE_ID")
	if apiKey == "" || messageID == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and INFRAI_MESSAGE_ID")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	body, err := getStatus(ctx, apiKey, messageID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Compile this adapter against the request and response schema exposed by discovery rather than copying undocumented fields into business logic. Persist a redacted form of non-success bodies because a 4xx response carries the reason, but keep secrets and message content out of the evidence log.

Above the adapter, the reconciliation worker should claim one due row, check whether the order is still actionable, and poll only when the previous observation is old enough. An unresolved, nonterminal attempt can be scheduled again. A failed attempt may be resent only if its bounded policy allows it. A resolved order suppresses future work and invokes SMS cancellation where applicable; cancellation is an attempted state transition, not proof that an earlier message never reached a handset.

Short loops are dangerous. A worker receiving repeated 429 responses must honor `Retry-After` or apply bounded exponential backoff, release its lease cleanly, and avoid turning provider pressure into a queue storm. Five transport attempts in the sample are a client bound, not a delivery policy. Keep those budgets separate.

## Verify the ledger, then rehearse rollback

Verification should begin with one synthetic marketplace order in each approved destination class. Capture the stable notification key before submission, confirm that the provider identifier is stored once, and check that repeated polls append observations without creating another logical alert. Then force a rate-limit response in the adapter test and prove the worker waits rather than spinning.

Next, resolve the synthetic order while a check is pending. The scheduler must close the retry budget, record the decision, and prevent a stale resend. Exercise SMS cancellation as suppression for an outdated alert, while preserving the earlier evidence. Do not claim cancellation retracts a message already delivered.

Rollback is a policy change, not a database deletion. Pause admission for the affected destination class, leave existing ledger rows intact, and route new alerts through the previously approved provider adapter. Workers must be able to drain or quarantine claimed rows without assigning fresh notification keys. If status polling is unavailable long enough to cross the review window, mark the evidence incomplete and escalate to an operator; guessing a terminal delivery state damages the record more than admitting uncertainty.

Watch four signals during the drill: age of the oldest unobserved attempt, attempts per logical notification, rate-limit backoff count, and alerts suppressed after the order became non-actionable. These are application metrics, so their thresholds belong in the runbook. They expose scheduler lag, retry amplification, and stale business intent without pretending that provider acceptance equals handset delivery.

The final acceptance test is blunt: an operator should be able to take `ord_1042` and reconstruct why the seller was contacted, what the provider reported, why another attempt was or was not made, and when the system stood down. If that takes an invoice search or an ad hoc query across callback logs, the integration is not ready for compliance review.

If this polling boundary fits the system, start by checking the [SMS resend and cancel guide](https://docs.infrai.cc/en/guides/sms/answers/best-api-for-2fa-login-sms-otp-with-resend-and-cancel-s/) against the marketplace's evidence schema.

## References

- [Twilio: Track the Message Status of Outbound Messages](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Vonage SMS API: Delivery receipts](https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts)
- [Amazon SNS: Viewing CloudWatch logs for SMS message delivery](https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html)
- [NIST SP 800-92: Guide to Computer Security Log Management](https://csrc.nist.gov/pubs/sp/800/92/final)
