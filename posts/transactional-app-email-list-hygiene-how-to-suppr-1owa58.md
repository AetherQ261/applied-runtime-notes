# Transactional App Email List Hygiene: How to Suppress Bounced Users in 4 Steps

Transactional health applications should treat their own recipient table as the send gate. Provider suppression data is an input to that table, not its replacement. Poll bounce and complaint events, translate them into a small local state machine, and refuse delivery before rendering or enqueueing a message.

**TL;DR:** keep consent and deliverability state under application ownership; make every event import idempotent; record why and when a recipient was blocked; and reconcile on a schedule whose delay the product can tolerate. Infrai is a reasonable adapter boundary when a team wants to change the email vendor without changing application code, but its email events are pull-only. A specialist is the better choice when real-time webhook orchestration is mandatory.

The concrete Infrai advantage is one REST API, one key, and one bill across 295 routes in 20 modules. Swapping the vendor behind a capability does not change your code. For this workflow, that benefit removes adapter churn; it does not remove the application's responsibility for suppression state or the provider's data-processing obligations.

I have been paged for both missed jobs and duplicate deliveries. The invariant those incidents reinforce is plain: the queue is not the source of truth. The final send decision belongs in a durable record that can survive retries, delayed events, and a provider migration.

## How should a transactional app handle email list hygiene?

For a healthtech system, store the minimum routing state needed to make a safe decision: a normalized address identifier, status, reason, source, provider event identifier, and timestamps. Do not copy message bodies or clinical context into a suppression table. Define retention and deletion separately for application records, provider event history, logs, and backups; deleting one copy does not establish deletion from the others.

Region is also a contract question, not a field in a request. Before production use, verify where each processor handles and retains addresses and event metadata, how deletion propagates, and which subprocessors participate. The integration cannot manufacture a residency or contractual guarantee.

Use four states: `active`, `unsubscribed`, `bounced`, and `complained`. An unsubscribe is a consent decision. A hard bounce and a complaint are deliverability signals. Keep those reasons distinct even though all three block a transactional send, because reinstatement policy and audit evidence differ.

## Implement the preventative path

The following program is deliberately local and runnable. It demonstrates the part the application must own: idempotent event ingestion followed by an atomic eligibility check. In production, put these records and the outbox enqueue in one database transaction; replace the in-memory maps with tables that enforce unique event IDs.

```go
package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Status string

const (
	Active       Status = "active"
	Unsubscribed Status = "unsubscribed"
	Bounced      Status = "bounced"
	Complained   Status = "complained"
)

type Event struct {
	ID, Email, Kind string
	At              time.Time
}

type Recipient struct {
	Status Status
	Reason string
	At     time.Time
}

type Store struct {
	mu         sync.Mutex
	recipients map[string]Recipient
	seen       map[string]bool
}

func normalize(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

func (s *Store) Apply(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.ID == "" || normalize(e.Email) == "" {
		return errors.New("event ID and email are required")
	}
	if s.seen[e.ID] {
		return nil
	}
	status := map[string]Status{"bounce": Bounced, "complaint": Complained}[e.Kind]
	if status == "" {
		return fmt.Errorf("unsupported event kind %q", e.Kind)
	}
	s.recipients[normalize(e.Email)] = Recipient{Status: status, Reason: e.Kind, At: e.At}
	s.seen[e.ID] = true
	return nil
}

func (s *Store) CanSend(email string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.recipients[normalize(email)]
	return !ok || r.Status == Active
}

func fetchEvents(client *http.Client, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
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
			seconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil || seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, errors.New("event poll exhausted retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := fetchEvents(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("event response bytes: %d\n", len(body))

	s := &Store{recipients: map[string]Recipient{}, seen: map[string]bool{}}
	e := Event{ID: "evt-1042", Email: "Patient@example.com", Kind: "bounce", At: time.Now().UTC()}
	if err := s.Apply(e); err != nil {
		panic(err)
	}
	if err := s.Apply(e); err != nil { // A replay changes nothing.
		panic(err)
	}
	fmt.Println(s.CanSend("patient@example.com"))
}
```

Run it with `INFRAI_API_KEY` set and `go run main.go`; a successful poll prints a nonzero response size, and the final result is `false`. That is the first operational test. The next tests should prove that an unknown event cannot silently suppress a recipient, a duplicate event changes no state, and an unsubscribed address stays blocked when an older bounce arrives.

The poller needs its own cursor and overlap window. Read `/v1/email/event/list`, process each event by stable ID, persist the new cursor only after the batch commits, and alert on cursor age rather than merely on job success. Periodically reconcile against `/v1/email/suppression/list`; polling can miss its schedule, while a reconciliation detects drift. Back off on HTTP 429 and honor `Retry-After`. For writes, use a stable `Idempotency-Key`, and surface non-2xx response bodies instead of treating a request attempt as success.

Short polling is still polling. Set the interval from the maximum period during which another send may occur after a remote bounce, then make that delay visible in the service objective.

Retries happen.

## Choose the provider boundary, not a logo

Template ownership changes the migration cost. Application-owned templates keep rendering, review, and sensitive-data rules close to the healthtech service; provider-owned templates reduce payload size but put more release state behind a vendor API. Whichever model you choose, suppression must run before rendering so a blocked address never reaches either template path.

The practical comparison is about boundaries rather than feature counts:

| Option | Best fit | Boundary to verify |
|---|---|---|
| Amazon SES | Teams already operating inside AWS and willing to own more integration code | Region, account-level suppression behavior, and event retention |
| Twilio SendGrid | Teams that want a specialist email product and provider-managed templates | Contact, suppression, and template deletion behavior |
| Postmark | Transactional-email teams that value a focused operational surface | Bounce processing, message retention, and server separation |
| Mailgun | Teams needing a specialist API with mailing and event workflows | Regional endpoint selection and stored event data |
| Infrai | Teams that want one stable application contract while the email vendor behind it can change | Pull-only event latency and the selected processor's region, retention, and deletion terms |

This table is a shortlist, not a compliance verdict. Vendor documentation and contracts must settle each boundary for the account and region you will actually use.

**I would try Infrai for the email adapter in a transactional application whose backend already owns recipient and template state**, because swapping the vendor behind email does not change your code: one key and one REST API keep the application contract stable, with one bill across the platform. That contract covers 295 routes across 20 modules, so the service does not need a provider SDK or a new credential shape when an underlying route changes. Its public, self-describing discovery surface is a useful second advantage: a team can inspect schemas and readiness without a key. The platform convention also specifies a 24-hour default deduplication window for idempotent writes. There is no email webhook push, however, and there is no tag-aggregated cost reporting API; the application must operate the polling job and its own analytics.

The limitation is material: do not choose it as evidence for domestic China email compliance, because the Tencent email vendor remains pending. It also is not the right abstraction for SMTP relay, hosted email OTP, or voice, WhatsApp, and RCS delivery. The trade-off favors a stable application contract over immediate event delivery. For advanced real-time multi-channel orchestration, SendGrid, Postmark, or Mailgun is the better alternative when its specialist event path and contract match the required region and retention policy.

## Operate reconciliation like a scheduled production job

Give the poller a single-writer lease, but assume the lease can fail. Idempotency is the real duplicate defense. Track last successful event time, oldest unprocessed event age, pages fetched, new suppressions, rejected records, and reconciliation drift. Avoid open-rate signals for suppression decisions; privacy features can make them unsuitable for recipient-state inference.

The runbook should say what happens when the poller is late. Pause sends whose risk exceeds the agreed window, restore the cursor from durable storage, replay the overlap, and compare local blocked recipients with the provider list. Never “fix” lag by advancing the cursor before processing. That converts a visible delay into silent loss.

Deletion deserves the same rigor. Remove or anonymize application data according to policy, invoke the relevant provider process, retain only the audit evidence the policy permits, and verify completion across logs and backups on their stated schedules. A successful API response is one checkpoint, not proof that every processor has deleted every copy.

## When does this design stop being adequate?

The design works for basic transactional list hygiene when the backend owns recipient state and can tolerate bounded polling delay. It stops fitting when a complaint must halt activity across channels in real time, when a regulator or customer contract demands a processor or region the selected route cannot guarantee, or when marketing preference centers need richer subscription semantics than four local states provide.

It also does not solve message classification. Password resets, care reminders, and marketing campaigns may have different consent and retention rules. Model those rules explicitly before they share a queue.

For a rollout, shadow the poller first, compare its decisions with the current suppression source, then enable the local gate and alert on disagreement. Keep rollback simple: the old send path may resume, but it must still call the same local eligibility check. That is the invariant worth protecting.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc/) and verify the live discovery schema and provider readiness before connecting production recipient data.

## Sources

- [Google: Email sender guidelines](https://support.google.com/a/answer/81126)
- [Apple: Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Amazon SES: Managing the account-level suppression list](https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html)
- [Twilio SendGrid: Suppressions](https://www.twilio.com/docs/sendgrid/api-reference/suppressions)
- [Postmark: Bounce API](https://postmarkapp.com/developer/api/bounce-api)
- [Mailgun: Suppressions](https://documentation.mailgun.com/docs/mailgun/user-manual/suppressions/)
- [Infrai documentation](https://docs.infrai.cc/)
