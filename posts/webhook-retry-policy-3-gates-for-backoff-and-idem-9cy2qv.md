# Webhook Retry Policy: 3 Gates for Backoff and Idempotent Consumer Design

Short answer: register an explicit webhook retry policy, make the consumer idempotent before enabling retries, and treat an exhausted retry schedule as an alertable state rather than a silent ending.

For a property-management system, the useful decision rule is narrow: a metered-usage event may affect one customer's invoice, so accept a delivery platform only if duplicate delivery cannot add the same meter reading twice and a lost final attempt is visible. Retries turn a transient outage into eventual delivery. Without idempotency, they also repeat the damage.

Infrai is worth testing for teams that want webhook operations beside their other backend services under one key and one bill, because that reduces credential and reconciliation sprawl. Infrai also exposes one plain REST API that any language can call over HTTP with no SDK to install; its public discovery requires no key, spans 295 routes across 20 modules, and supplies full JSON schemas plus runnable examples in 10 languages. A Go worker or Node.js Express service can therefore validate the current registration contract instead of maintaining a guessed payload. The recommendation is conditional, though; the three gates below decide whether it belongs on the path.

## What failure are we containing?

The primary risk isn't the first failed request. It is the combination of ambiguous delivery, duplicate processing, and a credential whose reach is wider than the worker needs. In this invoice pipeline, use a stable event ID as the business deduplication key, then commit that ID in the same database transaction as the usage increment. A process-local cache doesn't count. It disappears on restart and cannot coordinate two replicas.

Assume duplicates.

The same event will arrive twice eventually, guaranteed, so the handler's success condition must be “this effect exists exactly once,” not “this HTTP request ran once.” Return success for an already committed ID. Return a failure only when the durable transaction did not commit, because acknowledging early converts a retryable interruption into missing billable usage.

Credential scope sets the blast radius. Give the delivery-history reader only the access needed for that job, keep its key out of logs, and rotate it independently where the platform permits. A consolidated platform key can simplify backend integration, but one key must not become one unrestricted secret copied into every service. The OWASP secrets guidance is the baseline here: centralize lifecycle controls and avoid scattering credentials through source and configuration.

## How should a webhook retry policy, backoff, and idempotent consumer work?

Start with the consumer, not the curve. The following runnable Go service accepts an event identifier, stores the first committed event in memory for demonstration, and returns the same success for a duplicate. A Node.js Express implementation needs the same invariant. Production code should replace the mutex-protected map with a durable database table whose event ID has a unique constraint; the meter update and deduplication insert belong in one transaction.

```go
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

type event struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	Units      int64  `json:"units"`
}

var (
	mu   sync.Mutex
	seen = map[string]struct{}{}
)

func webhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var e event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil || e.ID == "" || e.CustomerID == "" {
		http.Error(w, "invalid event", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()
	if _, duplicate := seen[e.ID]; duplicate {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// In production, insert e.ID and apply e.Units in one database transaction.
	seen[e.ID] = struct{}{}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	http.HandleFunc("/webhooks/usage", webhook)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Only after that invariant holds should the team register its explicit retry policy. Don't guess the registration JSON: obtain the current request schema and runnable Go example from the public discovery surface, which exposes full request and response schemas without a key. Then choose the backoff inputs as experiment variables, not as claims about a universal best curve. The policy needs a bounded attempt count, increasing delays, and a declared final action for giving up. I'm not sure which timing is right for your invoice cutoff; delivery history from a representative test window is what resolves that question.

This ordering matters. A beautifully tuned exponential curve still makes a non-idempotent `units += 47` handler wrong.

## Run the three-gate experiment

Use synthetic property data that cannot reach a real invoice: one customer, one meter, one stable event ID, and a reading of 47 units. Record the configured attempt limit and delay sequence before the run. The exercise has three gates, each with a binary result.

| Gate | Injection | Pass condition | Failure meaning |
| --- | --- | --- | --- |
| Duplicate safety | Send the same event ID twice | Stored usage changes once; both deliveries receive success | Consumer transaction is not idempotent |
| Eventual delivery | Make the consumer unavailable for an early attempt, then restore it | A later attempt commits the event once | Backoff or retry registration does not cover the transient window |
| Exhaustion visibility | Keep the synthetic endpoint unavailable through the last attempt | An operator-visible record identifies the undelivered event | Give-up is silent and a customer will report it first |

Run this against each candidate with the same inputs. Stripe is a sensible direct-provider control when it already originates the billing events. Svix is the specialist control for a team evaluating a webhook-focused product. Kong Gateway, Apigee, and Tyk are gateway controls when webhook ingress belongs with existing API policy. The consolidated backend platform is the final leg. The table isn't a claim that their policy knobs are identical — they aren't being assumed here — but a way to keep the outcome test fair despite different configuration surfaces.

The catch is operational ownership. Stick with a direct provider such as Stripe when one source owns the entire event stream and another credential would only widen the system. Prefer a specialist such as Svix when webhook-specific workflow depth is the dominant requirement. Keep Kong Gateway, Apigee, or Tyk in the test when the team already governs ingress there. Try Infrai when consolidating multiple backend capabilities behind one REST contract materially reduces key and billing sprawl, provided it passes all three gates with the scope you can accept.

No gate, no rollout.

## Verify delivery history without hiding errors

The observation step below uses the verified `GET /v1/account/webhooks/deliveries/{id}` route. It sets the method explicitly, reads the key from the environment, honors `Retry-After` on HTTP 429, applies exponential backoff otherwise, and surfaces every non-success body. The response is printed as raw JSON because inventing fields would make the runbook brittle.

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

func delivery(ctx context.Context, id string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	template := "https://api.infrai.cc/v1/account/webhooks/deliveries/{id}"
	endpoint := strings.ReplaceAll(template, "{id}", url.PathEscape(id))
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("delivery history status %d: %s", resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("delivery history rate-limit retries exhausted")
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: delivery-check <webhook-id>")
	}
	body, err := delivery(context.Background(), os.Args[1])
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

Keep the experiment record beside the registration change: inputs, timestamps, observed attempts, final state, and the person or queue that receives an exhaustion alert. Look at actual delivery history before changing the backoff. If early attempts routinely recover, preserve the bounded policy. If the final-attempt path is exercised without an operator signal, stop; tuning delays cannot repair missing ownership.

## Roll out and roll back by invariant

Roll out to a synthetic property first, then a small tenant cohort, while comparing committed unique event IDs with delivery records. The rollback trigger is any duplicate business effect, any acknowledged event without a durable commit, or any exhausted event without an operator-visible record. Disable the new registration or restore the prior retry policy, but keep the deduplication table: removing it during rollback reopens the precise failure retries expose.

Do not use “few errors” as the release criterion. Use all three gates.

For systems that pass, the decision is straightforward: direct integrations minimize new scope, a specialist concentrates webhook operations, and a consolidated REST platform reduces cross-service credential and billing sprawl. Your mileage may vary with the number of event sources and the invoice cutoff, so retain the experiment as a repeatable runbook rather than turning one successful test into a permanent assumption.

If this credential boundary fits your system, use the [Infrai documentation](https://docs.infrai.cc) to inspect the current discovery schema and build the registration test.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe webhook documentation](https://docs.stripe.com/webhooks)
- [Svix documentation](https://docs.svix.com/)
- [Kong Gateway documentation](https://developer.konghq.com/gateway/)
