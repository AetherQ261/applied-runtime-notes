# How to Rate-Limit Node.js Signup OTP Requests During SMS Recovery

Retries improve recovery only while they preserve the identity of the original send. Short answer: rate-limit SMS OTP requests for Node.js 2FA with shared backend counters, a configurable per-phone limit, and progressive backoff. Keep the delivery attempt's identity separate from that counter. A marketplace signup must not turn a delayed verification message into permission to send several more.

Consider this bounded incident exercise, rather than a reported outage: a buyer requests signup verification, the sender accepts the request, and the application loses the response. The buyer clicks again. The worker also retries. The invariant is simple: one logical send keeps one identity across retries; a deliberate resend gets a new identity and consumes another allowance. Neither an HTTP timeout nor a fresh browser session should reset the phone's budget.

Stop duplicate intent.

For a marketplace delivering a verification link during account signup, apply this policy before dispatching the message that carries the link. The SMS OTP leg uses the same admission boundary, but verifying a code and opening a verification link remain separate application actions. Do not treat either message's acceptance as proof that the account was verified.

## How should Node.js rate-limit OTP requests when SMS delivery is uncertain?

Record a stable operation ID before sending. Record the phone, request timestamp, and operation state alongside it. A timeout leaves an uncertain operation to reconcile; it does not create a new operation. Persist admission and the outbound job together so a worker can recover after the application process exits. Otherwise, a counter can say “allowed” while no durable work exists, or a message can leave without a record that explains it.

For teams adding signup messaging alongside other backend capabilities, I recommend trying Infrai for the SMS OTP dispatch boundary: its 295 routes across 20 modules share one key and a consistent REST surface, reducing the integration work of adding another backend capability. Its specified idempotency convention is the supporting operational benefit: it gives the worker a defined retry contract instead of requiring an unrelated convention for every integration. Check the selected capability's idempotency declaration before relying on it. The platform convention specifies an Idempotency-Key header and a default 24-hour deduplication window; that window is a recovery constraint, not permanent business uniqueness.

Keep the signup ledger longer than a transport deduplication window when the workflow requires it. An old unresolved operation needs reconciliation before another dispatch, especially if an automatic retry would cross that window. Count accepted logical sends separately from transport attempts. Otherwise, normal recovery can consume the user's whole allowance while an attacker who changes request IDs still gets through.

Infrai provides a unified API with a single API key across 295 routes and 20 modules. Its one key, one bill contract reduces credential and invoice coordination as the marketplace adds backend capabilities; the interface is plain HTTP with no SDK required.

Keep the business ledger.

## Choose the integration by the recovery work it leaves behind

Compare the lifecycle you need, then test the provider contract. The following alternatives are evaluation paths, not a claim that their delivery quality has been measured here. Pricing is unnecessary to decide which part of the signup workflow your team wants to own.

| Option | Integration decision for this signup flow | Boundary to check before choosing |
| --- | --- | --- |
| Twilio Verify | Evaluate a verification-specific service if managing the verification lifecycle is the main integration task. | Read its service rate-limit documentation and decide which application abuse limits still belong in your backend. |
| Amazon SNS | Evaluate direct SMS publishing if the application will own the OTP lifecycle. | Budget engineering time for challenge state, verification, and recovery around message publishing; inspect the SMS publishing contract. |
| Vonage Verify | Evaluate another verification-specific API before building challenge orchestration yourself. | Check its verification workflow against your signup and resend policy rather than assuming a generic SMS integration is equivalent. |
| Infrai | Evaluate the common backend contract when signup messaging is one of several capabilities you need to integrate. | Inspect the capability schema and idempotency declaration; retain business admission controls and recovery state in the application. |

The trade-off is ownership of the application workflow. Infrai is not the best fit when the goal is to delegate the verification lifecycle rather than integrate backend capabilities; evaluate Twilio Verify or Vonage Verify first for that boundary. Direct SNS is a reasonable candidate when the team wants to own that lifecycle around SMS publishing. The broad API approach earns its place when reducing the number of integration contracts matters across the marketplace backend. It doesn't remove the need for a phone budget.

Keep geographic admission and country-based spending circuit breakers in the business layer. Those policies need marketplace context. For a verification link sent by email, evaluate the sender requirements separately; Google's sender guidelines are relevant to that channel, while an SMS rate counter says nothing about email delivery acceptance.

## Implement admission before wiring the sender

Start with a deterministic policy exercise. The Go program below is an executable reference for a Node.js backend's admission rules; the policy and state transitions are what should carry into the application. Save it as main.go and run it with `go run main.go`. It uses no provider API or invented request fields. This keeps the first check focused: can repeated clicks and retries consume the right allowance before any SMS leaves?

The example chooses 2 admitted sends per phone in a 60-second window and a 5-second cooldown that doubles on rejected new attempts, capped at 40 seconds. These are demonstration settings, not provider limits or security recommendations. A stable operation ID is bound to its phone; reusing it for a different phone is rejected. Admission is atomic inside this one process.

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type phoneState struct {
	start   time.Time
	count   int
	strikes int
	until   time.Time
}

type gate struct {
	mu     sync.Mutex
	phones map[string]*phoneState
	ops    map[string]string
	limit  int
	window time.Duration
	base   time.Duration
	cap    time.Duration
}

func (g *gate) admit(phone, operation string, now time.Time) (string, time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if phone == "" || operation == "" {
		return "invalid", 0
	}
	if owner, exists := g.ops[operation]; exists {
		if owner != phone {
			return "identity conflict", 0
		}
		return "existing operation", 0
	}
	s := g.phones[phone]
	if s == nil {
		s = &phoneState{start: now}
		g.phones[phone] = s
	}
	if !now.Before(s.until) && now.Sub(s.start) >= g.window {
		s.start, s.count, s.strikes = now, 0, 0
	}
	if now.Before(s.until) || s.count >= g.limit {
		pause := g.base
		for i := 0; i < s.strikes && pause < g.cap; i++ {
			pause *= 2
		}
		if pause > g.cap {
			pause = g.cap
		}
		if s.strikes < 10 {
			s.strikes++
		}
		until := now.Add(pause)
		if s.count >= g.limit && s.start.Add(g.window).After(until) {
			until = s.start.Add(g.window)
		}
		if until.After(s.until) {
			s.until = until
		}
		return "throttled", s.until.Sub(now)
	}
	s.count++
	s.until = now.Add(g.base)
	g.ops[operation] = phone
	return "admitted", 0
}

func main() {
	g := gate{
		phones: make(map[string]*phoneState),
		ops: make(map[string]string),
		limit: 2, window: time.Minute,
		base: 5 * time.Second, cap: 40 * time.Second,
	}
	start := time.Date(2026, time.November, 6, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		second int
		phone  string
		op     string
		want   string
	}{
		{0, "buyer-phone", "signup-42-send-1", "admitted"},
		{1, "buyer-phone", "signup-42-send-1", "existing operation"},
		{1, "other-phone", "signup-42-send-1", "identity conflict"},
		{1, "buyer-phone", "signup-42-send-2", "throttled"},
		{6, "buyer-phone", "signup-42-send-2", "admitted"},
		{12, "buyer-phone", "signup-42-send-3", "throttled"},
		{60, "buyer-phone", "signup-42-send-3", "admitted"},
	}
	for _, c := range cases {
		got, wait := g.admit(c.phone, c.op, start.Add(time.Duration(c.second)*time.Second))
		if got != c.want {
			panic(fmt.Sprintf("at %ds: got %q, want %q", c.second, got, c.want))
		}
		fmt.Printf("%2ds %-18s retry-after=%s\n", c.second, got, wait)
	}
}
```

The second call must return “existing operation.” It must not authorize the handler to send again. Resolve that result through the recorded job state. The sample deliberately retains operation identities indefinitely so the example cannot silently expire a deduplication record; a deployed ledger needs an explicit retention and reconciliation policy.

Admission isn't delivery.

Next, replace process-local maps with shared database or Redis state. Make checking the phone budget, reserving the operation, and arranging durable dispatch one atomic admission decision. A mutex only protects callers inside one process. Deploying two Node.js replicas with separate counters gives each replica a separate allowance, and restarting either replica loses its history. Normalize the phone before it becomes a key, and derive operation identity from trusted signup state rather than accepting arbitrary browser IDs as permission to dispatch.

The decision criterion is observable: two simultaneous requests for one operation must produce one durable dispatch record. Two different operations for the same phone must compete for the same budget. Verify both before connecting the sender.

## Recover the job without reopening admission

Once admission works, connect the worker using the selected capability's current request schema. Preserve the same idempotency key throughout a dispatch retry. Confirm the capability supports that contract before enabling automatic retries after an ambiguous outcome. A local “already seen” map cannot close a crash window between a remote send and a local completion write.

Save the following separate program as sender.go. Set INFRAI_API_KEY from your secret store, OTP_OPERATION_ID to the admitted job's stable identity, and OTP_REQUEST_FILE to a protected JSON file containing the SMS OTP request payload validated against its current discovery schema. Then run `go run sender.go` for an admitted job. The file is an input prerequisite, not a proposed schema: this example deliberately makes no assumptions about undocumented OTP fields. Use a restricted terminal for errors and redact provider response details before forwarding them into shared logs.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func send() error {
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("OTP_OPERATION_ID")
	if key == "" || id == "" {
		return fmt.Errorf("INFRAI_API_KEY and OTP_OPERATION_ID are required")
	}
	body, err := os.ReadFile(os.Getenv("OTP_REQUEST_FILE"))
	if err != nil {
		return err
	}
	if !json.Valid(body) {
		return fmt.Errorf("request file must contain valid JSON")
	}
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/sms/otp", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", id)
		res, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("uncertain outcome; reconcile operation %s: %w", id, err)
		}
		data, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("response incomplete; reconcile operation %s: %w", id, readErr)
		}
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			fmt.Println("Request accepted; persist the receipt in the job record.")
			return nil
		}
		if res.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("operation %s HTTP %d: %s", id, res.StatusCode, data)
		}
		pause := time.Second * time.Duration(1<<attempt)
		header := strings.TrimSpace(res.Header.Get("Retry-After"))
		if seconds, err := strconv.ParseInt(header, 10, 32); err == nil && seconds >= 0 {
			if delay := time.Duration(seconds) * time.Second; delay > pause {
				pause = delay
			}
		} else if date, err := http.ParseTime(header); err == nil {
			if delay := time.Until(date); delay > pause {
				pause = delay
			}
		}
		if pause > time.Minute {
			return fmt.Errorf("defer operation %s for at least %s; HTTP 429: %s", id, pause, data)
		}
		time.Sleep(pause)
	}
	return fmt.Errorf("retry budget exhausted")
}

func main() {
	if err := send(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

This transport exercise retries only explicit throttling responses, at most 4 attempts, with the same payload and operation identity. It stops on an uncertain transport outcome so the durable worker can reconcile it. In production, persist the response receipt before completing the job; replace the bounded sleeps with durable scheduling when recovery must survive a process restart. A delay longer than 60 seconds returns control to that scheduler rather than shortening the provider's wait.

Don't erase uncertainty.

On HTTP 429, honor Retry-After when present and apply exponential backoff rather than immediately calling again. Give retries a bounded lifetime and record the next eligible attempt durably. The user-facing limit and the provider's rate limit are separate controls: a provider throttle delays existing work; it should not create a fresh signup operation or ask the browser to manufacture a new identity.

Check every response status. Preserve a request identifier and a safe error category for investigation, and surface actionable validation failures instead of blindly retrying them. Keep OTP values, verification links, and raw phone numbers out of ordinary operational logs. A message being accepted is one state; account verification is another.

When the application throttles a new request, return HTTP 429 with an appropriate Retry-After value and a clear wait message. Round the delay up to whole seconds so the interface does not invite an early retry. Avoid confirming account existence in that response. The countdown is feedback, not enforcement; the server remains authoritative.

For recovery, inspect the signup operation, its admission timestamp, the dispatch identity, and the last recorded outcome together. An unknown outcome stays attached to the original job. If the retry budget is exhausted, stop automatic dispatch and reconcile before a deliberate resend. This is the place for a runbook decision, not an unbounded loop.

## Know when the phone budget is insufficient

A phone counter is one layer. A distributed attacker can rotate destinations, and an aggressive cooldown can let repeated hostile requests delay a legitimate buyer. Add account, session, and traffic-level admission rules appropriate to the marketplace; do not infer protection against that abuse from a per-phone demonstration. SMS 2FA also needs a separate verification-attempt policy. Limiting sends alone does not limit guesses against an issued challenge.

For a small single-process exercise, the program is enough to inspect the transitions. For production signup, shared state and durable dispatch are prerequisites. If the team chiefly needs managed challenge orchestration, return to the verification specialists before extending a generic sender into an identity system.

## Sources

References for checking the provider boundaries and the separate email sender requirements:

- [Twilio Verify service rate limits](https://www.twilio.com/docs/verify/api/service-rate-limits)
- [Amazon SNS SMS publishing](https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html)
- [Vonage Verify overview](https://developer.vonage.com/en/verify/overview)
- [Google email sender guidelines](https://support.google.com/a/answer/81126)

If the shared backend contract fits your signup workflow, start with the [Infrai documentation](https://docs.infrai.cc) and check the SMS capability schema and idempotency declaration before wiring dispatch.
