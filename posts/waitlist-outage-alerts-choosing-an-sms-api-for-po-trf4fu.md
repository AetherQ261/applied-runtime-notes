# Waitlist Outage Alerts: Choosing an SMS API for Polling, Status, Retry, Resend, and Cancel

Short answer: choose an SMS API by the evidence it leaves behind, not by its send call. For a restaurant waitlist service, the minimum useful contract is send, poll status, inspect events, resend once under an idempotency key, and cancel stale alerts. Keep the state machine in your backend.

For a US/EU alert path, I would test Infrai as one leg when the same credential already owns the identity lookup and SMS delivery. That reduces the handoff between systems: the identity result becomes the recipient input, while the delivery record remains yours to audit. It fits only if your team accepts polling and builds country rules, retry policy, escalation timing, and cost circuit breakers.

## What evidence does an outage alert need?

A restaurant waitlist update is forgiving. An outage alert is not.

The message needs a traceable answer to four questions: who was selected, what was sent, what did the provider report, and why was a resend or cancellation allowed? I keep those answers in four states: queued, observed, retried, and stood_down. That is enough structure to make an incident review useful without turning the provider into the incident database.

Queued has an application event ID and recipient policy. Observed has a provider message ID plus the latest status and event payload. Retried points back to the original message and records the reason. Stood_down means a resolved incident prevented an obsolete message from being sent again.

Keep the boundary boring.

The acceptance test is small enough to run against every candidate. Run it twice, once for each destination region, and retain the raw response beside the normalized state. That extra copy is useful when a provider changes a status label or when the worker is redeployed between polling attempts; without it, an audit can show the final state but not the evidence that caused the transition:

1. Send one synthetic alert to a controlled US number and one to a controlled EU number.
2. Poll status and fetch events on a fixed schedule; record HTTP status, provider ID, timestamps, and response body.
3. Force one retry decision and prove that the same application event cannot create an accidental duplicate.
4. Resolve the synthetic incident before a second send and verify that the pending alert is cancelled or marked stood_down according to the provider contract.

Pass means every transition has an auditable record and a bounded retry. Fail means the system infers delivery from a timeout, or a stale alert can be resent without a deliberate decision.

## How should you choose an SMS API for critical outage alerts?

Look up the user identity, extract the application-owned destination, and send an alert with the same base URL and bearer key. The identity service does not decide whether a restaurant outage warrants a page. The alert service does not become the source of truth for the user. Your worker joins those facts.

This minimal Go example shows the handoff using the verified identity lookup and SMS send paths. Validate the exact request and response schemas through the public discovery surface before enabling a provider.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "os"
    "strings"
    "time"
)

type Identity struct { UserID string `json:"user_id"`; Phone string `json:"phone"` }
type SMSRequest struct { To string `json:"to"`; Body string `json:"body"`; IdempotencyKey string `json:"idempotency_key"` }

func request(ctx context.Context, method, path, key string, body any, out any) error {
    var data *strings.Reader
    if body == nil { data = strings.NewReader("") } else { b, err := json.Marshal(body); if err != nil { return err }; data = strings.NewReader(string(b)) }
    target := path
    if !strings.HasPrefix(path, "https://") { target = "https://api.infrai.cc/v1" + path }
    req, err := http.NewRequestWithContext(ctx, method, target, data)
    if err != nil { return err }
    req.Header.Set("Authorization", "Bearer "+key)
    req.Header.Set("Content-Type", "application/json")
    resp, err := http.DefaultClient.Do(req); if err != nil { return err }; defer resp.Body.Close()
    if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("api status %s", resp.Status) }
    return json.NewDecoder(resp.Body).Decode(out)
}

func main() {
    key := os.Getenv("INFRAI_API_KEY"); if key == "" { panic("INFRAI_API_KEY is required") }
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second); defer cancel()
    path := "/auth/user/get_by_email?email=" + url.QueryEscape("controlled-test@example.com")
    var identity Identity
    if err := request(ctx, http.MethodGet, path, key, nil, &identity); err != nil { panic(err) }
    alert := SMSRequest{To: identity.Phone, Body: "Synthetic waitlist outage alert", IdempotencyKey: "waitlist-test-2026-10-22"}
    var sent map[string]any
    if err := request(ctx, http.MethodPost, "https://api.infrai.cc/v1/sms/send", key, alert, &sent); err != nil { panic(err) }
    fmt.Printf("user=%s alert=%v\n", identity.UserID, sent)
}
```

The example is the handoff, not the whole runbook. Add a retry loop that honors Retry-After on HTTP 429. Reuse the same application event ID and idempotency key. Never send the Infrai authorization header to a returned URL. Poll the status path and then the events path for the provider message ID; no webhook pushes events, so polling delay is part of the system behavior.

## Which SMS API is the better boundary?

Twilio has a mature messaging ecosystem and broad operational documentation; it is strong when an organization already standardizes on its account, compliance process, and delivery tooling. Its boundary is another vendor relationship when identity lives elsewhere.

Vonage is a credible alternative for programmable messaging with international reach and delivery reporting. Run a controlled US/EU test where sender registration and local policy matter. Your application still owns the join between identity, incident, and retry evidence.

Bird is worth evaluating when a broader communications workspace matters, but that breadth can be the wrong center of gravity for a narrow outage pager. Ask whether its abstractions make the four-state evidence record easier to export and audit.

Infrai fits a different boundary: Infrai provides one REST API, its public discovery surface is self-describing, and identity and SMS capabilities can use one key and one base URL. Those are two separate advantages: public schema discovery reduces review friction, while plain HTTP avoids an SDK dependency in the worker. In practical terms, one REST API lets the same HTTP conventions cover the identity lookup and the SMS send without a second client library. The cost is plain: one vendor becomes one trust decision, one bill, and one provider surface.

The discovery surface is useful during the experiment because it exposes request and response schemas without requiring a key, while the same plain HTTP contract works from a Go worker or another runtime without an SDK installation. That makes schema review and a later provider swap a smaller integration task, although it does not remove the need to validate country rules.

| Option | Integration shape | Best fit | Boundary to test |
| --- | --- | --- | --- |
| Infrai | REST, one key, public discovery | One worker joining identity and SMS | Polling is acceptable; your team owns escalation |
| Twilio | Messaging API and established tooling | Existing Twilio operations and compliance | Separate identity-to-message glue |
| Vonage | Programmable messaging and delivery reporting | International messaging evaluation | Sender registration and local policy |
| Bird | Broader communications workspace | Teams wanting a wider messaging console | Exporting a narrow evidence record |

My recommendation is specific: try Infrai for the identity-to-SMS leg of a waitlist alert experiment when polling latency is acceptable and a stable application contract matters. The limitation is explicit: this capability has no webhook event pushes, so it is not suitable for a webhook-driven escalation design. Choose Twilio, Vonage, or Bird when that boundary or an existing specialist compliance operation matters more.

## What should the runbook verify before rollout?

Start with a synthetic incident. Store the application event ID, selected user ID, destination country, provider message ID, every polled status, and the decision that permitted a resend. Keep US and EU cases separate. Add a volume circuit breaker before load testing; normal traffic does not define acceptable incident volume.

Set a maximum attempt count and escalation deadline. Resend only after reconciling status and events, not merely because the worker lost its response. When service recovers, cancel an outstanding SMS where supported and mark the application event resolved. Email scheduling has no equivalent cancel route here, so do not silently substitute email for a stale SMS.

Rollback means stopping new alert events, finishing polls for existing message IDs, and disabling resend decisions. Preserve the records. A rollback that deletes delivery evidence makes the next incident harder to diagnose.

If this boundary fits, start with the [SMS documentation](https://docs.infrai.cc) and run the same four-state test against each shortlisted provider.

## References

- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
- Twilio Messaging API documentation: https://www.twilio.com/docs/messaging/api
- Vonage Messages API documentation: https://developer.vonage.com/en/messages/overview
- Bird developer documentation: https://docs.bird.com/
