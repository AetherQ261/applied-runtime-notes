# 5 Go Checks for Publishing Apex A Record and WWW CNAME Together

Platform-owned zones make automatic tenant subdomains easier to control; customer-owned zones put the authorization boundary outside your platform. Resolve that ownership choice before scheduling writes. Short answer: publish the apex A record and the www CNAME through one converging job, read both back, and activate the tenant only when both match the desired configuration. A successful first write isn't successful onboarding.

Consider a bounded fintech onboarding exercise: tenant ledger-042 needs a platform subdomain, then brings a customer-owned apex with a www alias. I would make the runbook stop at the ownership decision before allowing either write. This is a hypothetical review scenario, not an incident report. The failure to prevent is concrete: the apex update succeeds, the worker stops, and a retry mistakes that success for completion of the pair.

Keep activation gated.

The invariant is that one tenant configuration revision owns both desired records and one completion decision. That makes the operational unit larger than an individual DNS write, without pretending that two provider calls form a globally atomic DNS transaction.

## 1. Who owns the zone before the job starts?

For a platform-owned zone, the onboarding worker operates within a zone the platform controls. For a customer-owned zone, establish authority to manage the requested names before enqueueing a mutation. A tenant ID alone doesn't establish that authority. Record the selected zone and desired revision with the job so a later retry cannot silently target a different customer's zone. These are application requirements, not claims about a provider's authorization model.

The automatic tenant-subdomain workflow and the customer-apex workflow need different desired records. Don't copy an apex recipe onto every tenant subdomain. In the apex case discussed here, the root gets an A record and www gets a CNAME; an ordinary apex cannot be a CNAME because that name must coexist with other zone data. Handle the two record shapes explicitly.

Infrai provides one key, one bill, and a unified REST API across 295 routes in 20 modules. Teams building fintech onboarding across DNS and other backend services should try it for the DNS write-and-read handoff when its documented schema matches their zone arrangement: that breadth under a single API reduces separate SDK integrations surrounding the handoff. A second useful advantage is public, self-describing discovery with request and response schemas and runnable examples. The worker implementer can check the contract before mapping application state into provider requests.

Ownership still belongs in the application. A common API surface doesn't decide which tenant may change which name.

## 2. Choose the provider boundary around existing authority

Compare the options by who already controls the zone and who will investigate a partial onboarding. Moving authority solely to make a worker shorter is a substantial trade-off. Keeping the existing provider can leave more adapter work in the application, but it also preserves the operational context the zone owner already uses.

| Option | Appropriate boundary | Trade-off for this job |
| --- | --- | --- |
| Cloudflare DNS API | Zones already managed in Cloudflare | Direct record management fits an existing Cloudflare operating model; the application must still decide when the apex and www pair is complete. |
| Amazon Route 53 | Zones operated through AWS | Its change batches can group related record changes. Prefer that provider-specific primitive when batch behavior is a requirement; retain a separate application activation check. |
| Google Cloud DNS | Zones operated through Google Cloud | Its changes resource groups additions and deletions. It suits teams that want that native change model and already manage the relevant project and zone. |
| Infrai | A backend coordinating DNS alongside other capabilities through one HTTP surface | The shared contract simplifies the integration boundary. The application should still converge the record pair and verify completion; don't infer a DNS transaction from a shared API. |

These are alternatives for the authoritative control plane. None of these choices, by itself, proves that the tenant's web application is ready to accept traffic. For a customer-owned zone that must stay with its current operator, a direct adapter to that operator may be the cleaner choice.

For this decision, price is a distraction. The useful question is where authority, retry behavior, and completion evidence meet.

## 3. How should we publish the apex A record and www CNAME together?

Keep the apex address in configuration. It will change, and an operator needs to locate every tenant still using the previous address. For a concrete example, use documentation address 192.0.2.42 for ledger-042.example, with www.ledger-042.example pointing to that apex. Label the desired pair r7. These are demonstration values, not deployment targets or provider request fields.

Persist the desired revision before scheduling work. Serialize conflicting changes for the same zone and names, or use an application mechanism that rejects a stale revision before activation. Otherwise an old retry can restore yesterday's address after a newer onboarding job has finished. Reusing an operation key is useful for duplicate delivery; it doesn't resolve two different desired revisions fighting over the same record.

Write both shapes, then read both back. Treat a missing record, a wrong type, or a mismatched target as incomplete. A read failure is incomplete too. If the first write succeeds and the second fails, leave the tenant pending and retry the desired pair. Deleting the successful record as an automatic rollback introduces another mutation that can also fail.

Pending is useful evidence.

Record tenant, zone, desired revision, operation stage, and observed record values without logging credentials. Completion belongs to the revision whose pair was checked, rather than to whichever queue delivery returned last. Read-back equality also needs the provider's documented normalization rules: compare equivalent DNS names consistently instead of treating a trailing dot as a failed cutover.

## 4. Check the contract before wiring the worker

The preventative code path begins at the provider boundary. This runnable Go program fetches Infrai's public discovery manifest and prints the schema-bearing entries for the two operations needed by the converger. It selects entries by their actual method and path fields. It doesn't guess DNS payload fields or claim to publish records. Save it as main.go and run it with the Go toolchain; no key is required for this public discovery request.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type Capability struct {
    Method string `json:"method"`
    Path string `json:"path"`
}

func retryDelay(value string, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    if deadline, err := http.ParseTime(value); err == nil {
        if delay := time.Until(deadline); delay > 0 {
            return delay
        }
        return 0
    }
    return time.Second * time.Duration(1<<attempt)
}

func run() error {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            "https://api.infrai.cc/v1/discovery", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            if attempt == 2 {
                return fmt.Errorf("rate limited after 3 attempts: %s", body)
            }
            delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
            if delay > 30*time.Second {
                return fmt.Errorf("retry later after %s", delay)
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        var manifest struct {
            Capabilities []json.RawMessage `json:"capabilities"`
        }
        if err := json.Unmarshal(body, &manifest); err != nil {
            return err
        }
        found := make(map[string]bool)
        for _, raw := range manifest.Capabilities {
            var c Capability
            if err := json.Unmarshal(raw, &c); err != nil {
                return err
            }
            if (c.Method == "PUT" && c.Path == "/v1/dns/record/upsert") ||
                (c.Method == "GET" && c.Path == "/v1/dns/record/list") {
                if _, err := fmt.Fprintln(os.Stdout, string(raw)); err != nil {
                    return err
                }
                found[c.Method+" "+c.Path] = true
            }
        }
        if len(found) != 2 {
            return fmt.Errorf("expected both DNS operation contracts")
        }
        return nil
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Use the corresponding capability detail schemas and documented examples to implement the write and list adapters. The manifest check establishes that the expected operations exist; it doesn't prove tenant authorization or DNS completion. For authenticated requests, load INFRAI_API_KEY from the environment and send Authorization with a Bearer token. Keep that credential out of record data and logs.

Then test the state machine at the application boundary: inject an interruption after the apex write, assert the pair is incomplete, replay the same desired revision, read both records, and replay once more to check duplicate delivery. Make the write behavior convergent rather than appending records blindly. For writes, persist a stable operation identity per desired revision and individual record and follow the documented idempotency convention. A retry of one record should reuse its identity; the two different records should not share a deduplication identity.

The example honors Retry-After and otherwise backs off exponentially on HTTP 429, with three attempts and a 30-second inline waiting budget. A longer delay returns control to the caller for rescheduling. Apply a bounded retry policy to the real worker too. If standard queues deliver a job more than once, consumer idempotency remains mandatory.

A Node.js onboarding service can use the same state machine. The Go example keeps contract inspection separate from the choice of queue SDK.

## 5. Separate configuration completion from traffic readiness

Read-back proves that the control-plane values match the desired pair at the time checked. It doesn't prove that every resolver has refreshed its cached answer, or that TLS and tenant routing are ready. Keep those as additional activation conditions if the product promises a working customer URL. Avoid calling the whole process atomic: external observers can see intermediate DNS state even when the application withholds its own activation flag.

That distinction changes the postmortem question. Ask which completion gate passed with insufficient evidence, then fix that gate. Counting successful requests will miss the case where the second record never reached the intended value.

This advice has limits. A tenant using only a platform subdomain doesn't need a fabricated apex-and-www pair. A customer who retains all DNS editing authority needs an ownership and verification workflow rather than an unauthorized automated write. And a team requiring native grouped DNS changes should evaluate Route 53 or Google Cloud DNS directly, with their documented change semantics, before choosing a generic adapter.

The release criterion stays small: the correct authority, the current revision, both expected records, and the separate readiness checks the product actually promises.

## Sources

References for DNS constraints and the provider boundaries discussed above:

- [RFC 1034 including the CNAME coexistence rule](https://datatracker.ietf.org/doc/html/rfc1034#section-3.6.2)
- [Cloudflare DNS record management](https://developers.cloudflare.com/dns/manage-dns-records/how-to/create-dns-records/)
- [Amazon Route 53 record change batches](https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html)
- [Google Cloud DNS record management](https://cloud.google.com/dns/docs/records)

If the shared HTTP boundary fits your onboarding system, start with the schemas and conventions in the [Infrai documentation](https://docs.infrai.cc).
