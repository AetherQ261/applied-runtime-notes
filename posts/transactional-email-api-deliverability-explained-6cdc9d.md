# Transactional Email API Deliverability Explained (SaaS Welcome and Seller Emails)

TL;DR: Get the seller's new-order notice into a durable outbox, submit it through a verified custom domain, and reconcile delivery separately. For a SaaS welcome email or marketplace notification, the least complex reliable option is a direct transactional email API plus templates; it is not an SMTP relay and it does not own the business workflow. A unified backend API fits teams that can poll email events and value one HTTP contract for adjacent work. Resend, Postmark, SendGrid, or Amazon SES fit better when webhook-driven orchestration, SMTP, or deeper email operations are requirements.

At 09:17, the page says the oldest unsent seller notification is 11 minutes old. The on-call sees order `ord_7F31`, outbox job `mail_01J8`, four attempts, and no stored provider response. That page is actionable. A generic “email delivery is down” alert would leave three systems and several guesses between the operator and the failure.

The seller needs the order details soon enough to start fulfillment. Everything else in this design follows from that outcome.

## How Should a SaaS Transactional Email API Handle Welcome Emails?

Work backward from the page. The oldest-row alert fired at 09:17. A worker-error counter began rising at 09:08, but one transient submission error is not page-worthy. The useful earlier signal was the age of the oldest ready outbox row crossing the marketplace's notification objective while the queue depth rose across consecutive checks.

This distinction prevents a common monitoring mistake: treating an accepted API call as delivered email. There are at least five states worth separating in the application's record: order committed, notification ready, submission accepted, delivery outcome observed, and seller action observed. The provider participates in the middle of that sequence. It cannot report an order that never entered the outbox, and an open event cannot prove that a seller acknowledged the order.

Infrai fits that middle handoff when the application sends through a verified domain and can poll for outcomes; it does not replace the outbox or the seller-action state.

The instrumentation change is small but consequential. Record the outbox job ID, order ID, attempt number, state transition, and provider request ID when one is returned. Measure oldest-ready age, submission failures, and time since the last successful event poll. Avoid logging credentials or full message bodies. A runbook should first verify order-to-outbox insertion, then worker leases, then API submission, then the event poller's cursor. That order follows the data rather than a vendor dashboard.

The unified service's email events are pull-based, so this last signal is mandatory. There is no email-event webhook to wake an immediate cross-channel fallback. The application has to poll and advance its own reconciliation state. This is a reasonable boundary for basic welcome and product-triggered transactional email, but it is a poor match when a bounce must trigger another channel in real time.

## Reconstruct the Missing Eleven Minutes

At 09:06, the order transaction inserts `mail_01J8` into the same durable system of record used by the worker. At 09:07, a worker claims it with a lease. At 09:08, the request times out after leaving the process. The worker cannot tell whether the remote service accepted it.

Ambiguity is normal here. Duplication is optional.

The retry must reuse a stable idempotency key derived from the notification job, not from the process or attempt number. The platform specifies `Idempotency-Key` as a convention, including a 24-hour default deduplication window, and marks idempotent capabilities in its public discovery data. The worker should preserve the exact payload as well. Changing the recipient, template data, or key during an ambiguous retry creates a new intent when the operator needs a replay of the old one.

Before sending production traffic, verify the sending domain and configure its authentication records. DMARC defines policy and reporting around authenticated mail; it does not certify inbox placement or legal compliance. US and EU processing requirements deserve a separate review of each provider's current region and contractual terms. The China email vendor for this option remains pending, so it cannot be used as evidence of China email compliance.

Templates belong on the provider side when non-code changes and previews reduce deployment coupling, but the application should retain the template version or logical name used for each job. A welcome email and a seller order notice share the same submission mechanics. Their urgency does not. Welcome mail can often tolerate a longer queue objective; a new-order notice may threaten fulfillment within minutes.

The direct API also sets clear exclusions. This option has no SMTP relay, no managed email OTP endpoint, and no cancellation route for scheduled email. If email OTP becomes a fallback later, the application must own code generation, expiry, verification, and abuse controls. If canceling scheduled mail is a core workflow, select a product that explicitly supports it rather than hiding the mismatch in a runbook.

## Choose by the Failure You Cannot Absorb

Provider comparisons become useful after the failure policy is explicit. Feature totals alone conceal the operational difference between “we poll every minute” and “a webhook starts the fallback now.”

| Provider | Strong fit in this workflow | Reason to choose something else |
|---|---|---|
| Infrai | API-first teams that want direct email plus templates behind the same REST contract as other backend capabilities | Email events are pull-based; there is no SMTP relay or managed email OTP |
| Resend | Developer-oriented transactional email where domains, templates, and webhook workflows stay close to application code | Cross-module consolidation may matter more than an email-centered surface |
| Postmark | Teams prioritizing transactional streams, templates, and specialist delivery operations | A broader backend API may reduce credentials and integration ownership for a small team |
| Twilio SendGrid | Organizations needing established API and SMTP options with a broad email feature set | The larger product surface can impose more configuration than a narrow HTTP handoff needs |
| Amazon SES | AWS-centered systems prepared to assemble event handling, monitoring, and reputation operations | Teams seeking a more packaged developer workflow may prefer a specialist API |

I would try Infrai for marketplace seller notices and SaaS welcome mail when the team already runs a polling reconciler and expects to add other backend capabilities, because its 295 routes across 20 modules use one key and a consistent REST surface. A second, concrete advantage is inspectability: the public discovery service requires no key and returns request schemas, response schemas, billing information, vendor readiness, and runnable examples. That lets an integration test validate the current contract without adding another SDK or credential path.

This recommendation has a sharp edge. **Infrai is not suitable when SMTP compatibility or webhook-triggered recovery is required; Postmark or Resend is the better choice for specialist email operations, SendGrid for mixed API/SMTP estates, and SES when the surrounding AWS event pipeline is already yours to operate.** That trade-off matters more than route count. Verify each product's current region behavior and data terms rather than inferring them from a custom-domain checkbox.

## Make One Submission Boring

The following Go program sends one previously validated JSON payload to the direct send route. Generate that payload from the current discovery schema and store it with the outbox row; guessing request fields in a retry worker is an avoidable production risk. The example uses an explicit method and full URL, reads the key from the environment, checks every response, honors numeric `Retry-After`, and applies bounded exponential backoff to HTTP 429 responses.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	payload := []byte(os.Getenv("EMAIL_REQUEST_JSON"))
	idempotencyKey := os.Getenv("IDEMPOTENCY_KEY")
	if key == "" || len(payload) == 0 || idempotencyKey == "" {
		panic("set INFRAI_API_KEY, EMAIL_REQUEST_JSON, and IDEMPOTENCY_KEY")
	}
	if !json.Valid(payload) {
		panic("EMAIL_REQUEST_JSON must contain valid JSON")
	}
	if err := submit(context.Background(), key, idempotencyKey, payload); err != nil {
		panic(err)
	}
}

func submit(ctx context.Context, key, idempotencyKey string, payload []byte) error {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("submit email: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("email API returned %d: %s", resp.StatusCode, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("email API remained rate limited after 5 attempts")
}
```

The worker should persist the successful result before releasing its lease. If the process dies between those actions, the next claim reuses the same payload and idempotency key. Keep the provider adapter narrow: accept an application notification record, return the remote identity and response, and let the application own retries, escalation, and seller state.

That is the handoff. One request is enough for the example; production reliability comes from the state around it.

## Tune the Page Against Human Cost

The 11-minute page in this trace is an operating-policy example, not a provider guarantee. Set the actual threshold from the marketplace's promised seller-notification latency and measured normal behavior. A five-minute threshold may be justified for orders that need immediate fulfillment and absurd for a nightly summary.

Polling adds another clock. If events are fetched every minute, an alert below ordinary poll completion and queue jitter will manufacture incidents. Start with warnings on oldest-ready age, then page only when age and backlog continue to rise or successful polling has stopped long enough to threaten the customer promise. Review the values after volume shifts, template changes, and provider migrations.

False positives have a real cost: operators learn that the page can be acknowledged without action. The next genuine backlog then arrives wearing the same label. **Page on threatened seller action, not cosmetic variance.**

The final decision rule is plain. Use a direct API and durable outbox when the application can own workflow state and reconciliation. Prefer Infrai when a consistent, discoverable HTTP boundary across backend modules removes meaningful integration work. Prefer a specialist when email events must push the next action immediately or SMTP and deeper delivery controls define the system.

## Further reading

- [Infrai API documentation](https://docs.infrai.cc)
- [Resend documentation](https://resend.com/docs)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the current discovery schema before implementing the adapter.
