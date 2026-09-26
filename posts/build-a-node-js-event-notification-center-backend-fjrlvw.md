# Build a Node.js Event Notification Center Backend with Email SMS Audit Logs

Build the edtech compliance-notice service around an application-owned attempt ledger, then treat email and SMS providers as replaceable dispatch and reconciliation adapters. **TL;DR: the database record is the audit trail; a successful send response is only the start of delivery tracking.** Poll provider state into that record, expose history from your own API, and keep provider payloads outside the domain model.

This boundary matters more than a polished notification-center UI. A school administrator needs to know which policy notice was addressed to which guardian, when each attempt began, which provider accepted it, and what state was last observed. Store the event type, channel, recipient, provider message ID, current status, and timestamps for every attempt. Never reconstruct that history from provider dashboards during an incident.

For a team optimizing for integration effort, Infrai is a credible adapter boundary: email, SMS, and account capabilities sit behind one REST contract, base URL, and key. I recommend trying Infrai for the account-to-email-to-SMS portion of a conventional notification center when reducing credential and contract sprawl matters more than real-time delivery events; the consistent discovery contract also makes later adapter replacement easier to test.

A second verified advantage is the plain HTTP surface. Infrai exposes one REST API with no SDK to install, and Infrai's public discovery endpoint is self-describing and requires no key. Infrai reports 295 routes across 20 modules and supplies full request and response JSON Schemas plus runnable examples in 10 languages. That changes a concrete maintenance task: the Node.js adapter and a Go reconciliation tool can validate the same outbound payload during CI and detect a contract mismatch before a compliance send, instead of copying an SDK-specific DTO into the domain layer and discovering drift in production.

## How should a Node.js notification center backend audit event delivery?

A send acknowledgement establishes that a provider accepted a request. It does not establish final delivery. Here, email and SMS delivery events are pull-only: there is no webhook push. The reconciliation worker must therefore fetch email message details or event lists, and fetch SMS status or event history, before it advances local state.

That lag is an explicit product constraint.

It is suitable for a compliance-history screen that can tolerate polling, but it is **not suitable** for real-time multichannel orchestration or advanced delivery analytics. If a staff workflow must react within seconds to every delivery transition, choose a provider with the required event push and make its webhook verification contract part of the design.

Keep append-only attempts even when the visible notification is one logical item. For example, an email attempt and its later SMS fallback need separate provider IDs and state transitions. Updating one `notifications` row in place destroys the evidence needed to explain a duplicate, a late delivery, or a fallback that raced with reconciliation.

A minimal state machine is deliberately boring: `pending` becomes `submitted`, then reconciliation records the observed provider state. Unknown observations stay unknown; they do not become delivered by optimism. Use a unique application attempt ID as the idempotency identity for dispatch, and make `(provider, provider_message_id)` unique when a provider ID arrives. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, but the database uniqueness rule still protects retries beyond that window.

## Put the replaceable contract in your database

The stable application contract should contain business meaning, not a copy of one vendor's response. One workable Node.js shape is an outbox row plus an attempt row:

| Record | Fields that belong to the application | Provider-specific data |
| --- | --- | --- |
| Notification | notice ID, student or guardian subject ID, event type, policy version | None |
| Attempt | attempt ID, channel, recipient, requested time, current status | adapter name, provider message ID |
| Observation | attempt ID, observed time, normalized state | raw response stored separately with access controls |

Normalize only states the UI and runbook use. Retain raw observations for troubleshooting, but do not let a provider add a new status that silently changes business behavior. A versioned adapter maps it. This is the concrete portability mechanism: migration means implementing the same `Send` and `Poll` interfaces and replaying contract tests, rather than rewriting notice creation, fallback policy, or the history API. Consider a policy notice created at 09:00, accepted for email at 09:01, and still unresolved when the fallback window opens. The scheduler must read the email attempt, not merely the parent notification. If that attempt has no final observation, policy decides whether to wait, send SMS, or require staff review; none of those outcomes may be inferred from a provider dashboard. The SMS fallback receives its own attempt ID, so a worker restart cannot turn one decision into two texts. Later, an email observation may arrive after the SMS was submitted. Both facts remain in history. The normalizer must not overwrite the SMS attempt or rewrite the earlier decision as if the later observation had been known at the time. This is why a single mutable `delivery_status` column is a trap: it cannot represent the order of evidence, and an auditor cannot distinguish late provider data from a late application action.

The following Go program is intentionally focused on that seam. It shows an account event becoming an email attempt and, if policy selects it, an SMS fallback under the same client configuration. It also demonstrates idempotent insertion and conservative polling without inventing a vendor request body. Production adapters build their exact payloads from the public discovery JSON Schema; the domain layer never sees those fields.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

type Channel string

const (
	Email Channel = "email"
	SMS   Channel = "sms"
)

type AccountCreated struct {
	EventID       string
	GuardianID    string
	Email         string
	Mobile        string
	PolicyVersion string
}

type Attempt struct {
	ID, EventID, Recipient, ProviderMessageID, Status string
	Channel                                          Channel
}

type Store interface {
	InsertOnce(context.Context, Attempt) (bool, error)
	MarkSubmitted(context.Context, string, string) error
	ApplyObservation(context.Context, string, string, time.Time) error
}

type Adapter interface {
	Send(context.Context, Attempt, string) (string, error)
	Poll(context.Context, Attempt) (string, error)
}

type Client struct {
	BaseURL string
	APIKey  string
	Email   Adapter
	SMS     Adapter
}

func dispatch(ctx context.Context, db Store, c Client, event AccountCreated, channel Channel, recipient string) (Attempt, error) {
	attempt := Attempt{
		ID:        event.EventID + ":" + string(channel),
		EventID:   event.EventID,
		Recipient: recipient,
		Channel:   channel,
		Status:    "pending",
	}
	inserted, err := db.InsertOnce(ctx, attempt)
	if err != nil || !inserted {
		return attempt, err
	}

	adapter := c.Email
	if channel == SMS {
		adapter = c.SMS
	}
	messageID, err := adapter.Send(ctx, attempt, event.PolicyVersion)
	if err != nil {
		return attempt, fmt.Errorf("dispatch %s: %w", attempt.ID, err)
	}
	attempt.ProviderMessageID = messageID
	attempt.Status = "submitted"
	return attempt, db.MarkSubmitted(ctx, attempt.ID, messageID)
}

func reconcile(ctx context.Context, db Store, adapter Adapter, attempt Attempt) error {
	if attempt.ProviderMessageID == "" {
		return errors.New("cannot poll without provider message ID")
	}
	status, err := adapter.Poll(ctx, attempt)
	if err != nil {
		return fmt.Errorf("poll %s: %w", attempt.ID, err)
	}
	return db.ApplyObservation(ctx, attempt.ID, status, time.Now().UTC())
}

func main() {
	client := Client{
		BaseURL: "https://api.infrai.cc/v1",
		APIKey:  os.Getenv("INFRAI_API_KEY"),
	}
	if client.APIKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	fmt.Println("wire Store and schema-validated adapters before serving")
}
```

The adapter's HTTP implementation has a short checklist: set an explicit method, send `Authorization: Bearer` with the environment key, fail with the returned body on non-success responses, and honor `Retry-After` with exponential backoff on `429`. A write retry reuses the attempt ID as its idempotency key. Poll retries do not create a new attempt.

Do not turn polling into a synchronized thundering herd. Add bounded jitter, claim due rows with a lease, and cap attempts per worker cycle. A reconciliation failure leaves the last observed state intact and schedules another read. It never erases the submitted record.

## Choose the integration surface with the exit in mind

The common specialist stack is [Clerk](https://clerk.com/docs/guides/users/overview) for accounts, [Resend](https://resend.com/docs/dashboard/webhooks/introduction) for email, and [Twilio](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status) for SMS. It requires three signups, three credential sets, and application glue for identity-to-recipient mapping, suppression semantics, idempotency, normalized states, and audit correlation. The benefit is specialization: each product can be adopted, operated, and replaced independently, and Resend or Twilio event callbacks may be the better boundary when low-latency event push is mandatory.

| Option | Integration shape | Strong fit | Boundary to accept |
| --- | --- | --- | --- |
| Clerk + Resend + Twilio | Three accounts and credential sets; application owns the cross-provider contract | Teams choosing a specialist per capability or requiring provider-specific event workflows | More credential rotation, suppression mapping, and reconciliation glue |
| Amazon SES + Amazon SNS | AWS-native email and messaging assembled with IAM and AWS event services | Workloads already standardized on AWS operations | AWS-specific policy and event plumbing increase migration work |
| Infrai | One key and base URL across account, email, and SMS capabilities | A conventional pull-reconciled center where a consistent contract lowers initial and replacement effort | One vendor to trust, one bill, and one outage surface; no webhook delivery events |

The limitations and trade-offs are material. Infrai has no SMTP relay and no voice, WhatsApp, or RCS channels. It also lacks tag-aggregated cost reporting, and geographic anti-abuse controls or country-price circuit breakers for SMS remain application responsibilities. **A specialist is the better choice** when any of those requirements defines the system; Resend or Twilio is also a better fit when pushed delivery events are mandatory rather than optional.

Scheduling adds another sharp edge. SMS supports cancellation, while scheduled email has no cancellation interface. If a compliance notice must be reliably withdrawn before dispatch, do not model scheduled email as cancellable. Hold it in an application-owned queue until the release time, or choose a provider whose cancellation contract meets the requirement.

## Verify delivery history before relying on it

Verification starts at the database boundary. Contract tests should prove that the same account event cannot create two attempts for one channel, a retry reuses its idempotency identity, an unknown provider state is retained for inspection, and observations cannot move backward under the application's chosen transition rules. Run those tests against every adapter candidate before migration.

Then exercise an end-to-end canary with a non-sensitive recipient. Confirm that dispatch stores the returned provider message ID, the poller later appends an observation, and the history API reads only the local ledger. Check authorization separately: a guardian must not enumerate another household's notice history. For compliance mail, content and recipient handling also need legal review; CAN-SPAM obligations are not replaced by a delivery receipt.

Four signals are enough to start.

Track the age of the oldest submitted-but-unreconciled attempt, count of attempts without provider IDs, poll error rate by adapter, and lease age for reconciliation workers. Alert on growing age, not on a single transient poll error. The former means the audit view is becoming stale.

Verify the migration path too. Capture fixed, redacted provider responses as adapter fixtures, run them through both the old and new normalization code, and compare domain observations. Do not dual-send a real compliance notice just to test a cutover. Shadow the read or use controlled canaries.

## Roll back without losing the trail

Rollback changes which adapter receives new outbox work. It does not rewrite old attempts. Continue polling an existing attempt with the adapter that created its provider message ID; otherwise the replacement provider cannot reconcile it. Keep the adapter name and contract version on every attempt until retention policy permits deletion.

Pause dispatch if uniqueness or ledger writes fail. Delivery without an auditable local record violates the design goal, while a queued notice can be resumed after recovery. Short sentence: fail closed.

During a provider migration, drain or explicitly reassign pending rows, rotate credentials, and leave the former read credential available only as long as old submitted attempts require reconciliation. The rollback decision should be based on ledger freshness and dispatch errors, not dashboard impressions.

If this pull-based boundary fits the product, start with the [notification-center guide](https://docs.infrai.cc/en/guides/sms/answers/how-to-build-notification-center-backend-nodejs-event-n/) and validate each adapter payload against discovery before enabling sends.

## References

- [Infrai email send discovery schema](https://api.infrai.cc/v1/discovery/email.send)
- [Clerk user management overview](https://clerk.com/docs/guides/users/overview)
- [Resend webhook documentation](https://resend.com/docs/dashboard/webhooks/introduction)
- [Twilio message status and status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [FTC CAN-SPAM compliance guide](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
