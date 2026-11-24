# Tenant Table Against Live DNS: 2 Gates for Healthtech Hostname Cutovers

Reconcile tenant zone identifiers against a complete live DNS zone inventory before switching a healthtech mail hostname, and keep the previous configuration available for rollback. The deciding constraint is deliverability evidence: an inventory match establishes ownership consistency, but it does not establish that patient notification mail will arrive.

Short answer: schedule a reconciliation that lists zones, compares them with tenant records, and reports both orphan and missing zones as metrics. Gate the cutover on those results plus mail authentication and delivery checks. Alert on mismatches; let a human confirm the cause before changing anything.

Hold the switch.

Consider a patient appointment reminder hostname moving to a new configuration. The scheduled sender must not interpret a DNS change as permission to resend reminders. Keep the notification's application idempotency key stable through the switch and through rollback; a second delivery attempt is still the same business event. The example below uses synthetic zone identifiers and emits only inventory counts, so the reconciliation can be reviewed without putting patient data into its output.

## How should you reconcile a tenant table against a live DNS zone list?

There are two inventory gates. First, every tenant zone identifier expected in the selected scope must exist in the live inventory. Second, every live zone identifier in that scope must have an accountable tenant record. These are different questions. A missing zone is an outage concern; an orphan zone is a cost and ownership concern. Collapsing them into one drift total loses the distinction the operator needs.

Neither gate proves deliverability.

For this runbook, keep three artifacts with the change: the tenant and zone snapshots used for reconciliation, the approved previous and proposed hostname configurations, and the mail evidence used to authorize the switch. Treat those as operational requirements for this procedure. Use controlled test recipients to establish the acceptance evidence your team requires before patient reminders depend on the new hostname. RFC 7489 explains DMARC's authentication alignment and reporting model; a successful inventory comparison cannot substitute for that mail-specific evidence, and a successful authentication check alone cannot promise inbox placement.

A zone identifier should be the join key. A domain string can be re-pointed, so joining only on the visible hostname risks accepting the right name attached to the wrong resource. Keep the domain name as review context, but make resource identity decide membership. Scope identity to the provider and account when assembling the snapshot; the small example assumes one such scope and does not assume identifiers are globally unique.

This distinction also changes the postmortem question. Instead of asking whether a cron trigger fired, ask which completed inventory comparison and which delivery evidence justified the cutover. A trigger is an attempt. A completed, reviewable comparison is evidence.

## Schedule a comparison that cannot mutate DNS

Build the scheduled task around a complete tenant snapshot and a complete live zone list for the same intended scope. Follow the inventory source's documented listing and pagination behavior before comparing. If acquisition fails or the snapshot is incomplete, report a failed reconciliation rather than publishing a reassuring zero. An empty successful inventory and a failed inventory request must never share a success signal.

Start with reporting.

Use the scheduled run to produce two directional counts and retain the last successful completion time. The counts answer what differs; the completion time answers whether anyone has checked recently. Choose the schedule and freshness limit from the cutover procedure, rather than inventing a universal polling interval. During a change, require a successful comparison covering the configuration under review. A run completed before that configuration existed is stale evidence even if its timestamp looks recent. If inventory changes during acquisition, withhold the cutover decision until the operator can review a consistent scope; a comparison across different change states can manufacture apparent drift.

The executable Go example isolates the comparison from provider response formats. Its slices are synthetic inputs standing in for already acquired snapshots; it does not fetch live DNS or configure a scheduler. It emits metric-shaped JSON to standard output, which the surrounding job must publish through its chosen metrics integration. Duplicate zone identifiers in the live snapshot are rejected instead of silently inflating the inventory. Several tenant records may reference the same zone, so tenant expectations are treated as a set.

```go
package main

import (
    "encoding/json"
    "fmt"
    "os"
)

type Tenant struct {
    ZoneID string
}

type Counts struct {
    OrphanZones  int `json:"orphan_zones"`
    MissingZones int `json:"missing_zones"`
}

func reconcile(tenants []Tenant, live []string) (Counts, error) {
    expected := make(map[string]struct{})
    for _, tenant := range tenants {
        if tenant.ZoneID == "" {
            return Counts{}, fmt.Errorf("tenant snapshot has an empty zone ID")
        }
        expected[tenant.ZoneID] = struct{}{}
    }

    actual := make(map[string]struct{})
    for _, id := range live {
        if id == "" {
            return Counts{}, fmt.Errorf("live snapshot has an empty zone ID")
        }
        if _, exists := actual[id]; exists {
            return Counts{}, fmt.Errorf("live snapshot repeats zone ID %q", id)
        }
        actual[id] = struct{}{}
    }

    var counts Counts
    for id := range expected {
        if _, exists := actual[id]; !exists {
            counts.MissingZones++
        }
    }
    for id := range actual {
        if _, exists := expected[id]; !exists {
            counts.OrphanZones++
        }
    }
    return counts, nil
}

func main() {
    tenants := []Tenant{{ZoneID: "zone-appointment"}, {ZoneID: "zone-reminder"}}
    live := []string{"zone-appointment", "zone-unassigned"}
    counts, err := reconcile(tenants, live)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if err := json.NewEncoder(os.Stdout).Encode(counts); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The expected output contains 1 orphan zone and 1 missing zone. That is a fixture expectation, not a measurement of a production system. The comparison performs no writes, so rerunning it cannot remove a zone or duplicate a reminder. Publishing its result is a separate responsibility: use a stable logical run identifier for retried reports and apply the receiving system's documented idempotency behavior. Don't assume a metrics write and a notification send have interchangeable deduplication semantics.

The trade-off is deliberate: this runnable example proves the set comparison, while the inventory adapter must implement its provider's actual schema and completeness rules. It cannot certify a live cutover by itself. Keep acquisition failures visible to the operator and reject a partial input before it reaches this function.

## Choose the integration around evidence ownership

Compare Cloudflare DNS, Amazon Route 53, and Google Cloud DNS against the same acceptance task: acquire a complete inventory for the tenant scope, retain resource identifiers, and attach the reconciliation result to the cutover review. Their official documentation is listed below. This note does not rank their mail deliverability; the inventory exercise supplies no measured basis for such a ranking.

The useful differences here are deployment circumstances and integration ownership. These are selection rules for the proposed workflow, rather than an assertion that one product has a unique reconciliation feature.

| Option | Prefer it in this workflow when | Boundary to keep explicit |
| --- | --- | --- |
| Cloudflare DNS | The approved old and new configurations are already managed there, and that team can own the inventory adapter. | Still reconcile both directions and gather independent mail evidence. |
| Amazon Route 53 | The responsible operations team already owns the relevant account and its change review. | Account scope must match tenant expectations; the product name is not proof that the list is complete. |
| Google Cloud DNS | The cutover's project ownership and rollback approvals already sit with that team. | Document project coverage before interpreting missing resources as drift. |
| Consolidated backend | The same operator owns the inventory job, its schedule, and its reporting integration. | Consolidation must fit the rollback approval process; retain the same deliverability acceptance gates. |

Infrai offers one key and one bill, an SDK-free REST API, and 295 routes across 20 modules. Plain HTTP requests need no SDK installation. That reduces credential and invoice coordination for the job owner while letting the inventory reader, scheduler, and metrics publisher share a unified API instead of introducing three SDK integrations. Its public discovery is self-describing, with request and response schemas and runnable examples in 10 languages; the Go adapter owner can inspect the contract without a key before connecting this job to reporting.

One REST API covers DNS, scheduling, and observability: plain HTTP, no SDK required. For this workflow, that means the Go inventory adapter and metrics publisher do not need separate service SDKs. The broad backend API spans 295 routes across 20 modules with a simple, consistent interface. Public, self-describing discovery requires no key and exposes request and response schemas, letting the operator inspect the inventory contract before wiring the scheduled check to reporting. These are integration advantages. They do not supply evidence that reminder mail reached a recipient.

The following standalone Go program acquires the live inventory response. Set `INFRAI_API_ORIGIN` to the documented HTTPS API origin without a trailing slash or version suffix, and provide `INFRAI_API_KEY` through the job's secret configuration. The program prints the response without assuming undocumented response fields. A provider adapter must still decode it and establish list completeness before passing identifiers to the comparison above. Do not interpret a successful HTTP status alone as a complete snapshot.

The example sets a 30-second acquisition deadline and permits 4 attempts. These are proposed client limits, not service performance measurements. HTTP 429 triggers exponential backoff unless a valid `Retry-After` specifies the wait; a wait beyond the deadline fails acquisition. Redirects are disabled to keep the credential at its configured origin. This read-only request cannot create duplicate zones.

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
    "time"
)

func acquire() error {
    origin := os.Getenv("INFRAI_API_ORIGIN")
    key := os.Getenv("INFRAI_API_KEY")
    u, err := url.Parse(origin)
    if err != nil || u.Scheme != "https" || u.Host == "" ||
        u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
        u.User != nil || key == "" {
        return fmt.Errorf("set a valid HTTPS API origin and API key")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            origin+"/v1/dns/domain/list", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            _, err = os.Stdout.Write(body)
            return err
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("inventory HTTP %d: %s", resp.StatusCode, body)
        }
        wait := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds >= 0 {
            if seconds > 30 {
                return fmt.Errorf("retry wait exceeds acquisition budget")
            }
            wait = time.Duration(seconds) * time.Second
        } else if when, err := http.ParseTime(retry); err == nil {
            wait = time.Until(when)
            if wait < 0 {
                wait = 0
            }
        }
        timer := time.NewTimer(wait)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("inventory acquisition exhausted attempts")
}

func main() {
    if err := acquire(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Keeping an existing provider can be the better choice when moving responsibility would make the rollback harder to approve. Don't migrate control planes merely to implement a set comparison. If the DNS owners already have an approved listing adapter and change workflow, use it. No option earns an exemption from delivery verification.

## Verify the switch against the actual reminder workflow

Before approving the cutover, run the comparison against the configuration under review and inspect both counts. A missing zone blocks approval pending investigation. An orphan needs an accountable disposition; it may reflect an intentional resource outside the tenant snapshot, a scope error, or an unintended addition. Do not turn the first mismatch into a delete operation. Record what the operator decided and which snapshots supported that decision.

Then check mail.

Define the controlled recipient set and acceptance criteria before the change, and preserve the authentication and delivery observations with the cutover record. Use the same criteria when examining the proposed configuration. DMARC reporting and alignment are relevant evidence, but the inventory job must not label a hostname deliverable merely because its zone exists. A reminder accepted for processing and a reminder observed at the intended test recipient answer different operational questions. This procedure deliberately requires the team to identify which observations authorize patient traffic, rather than assigning an invented universal threshold.

Keep scheduled reconciliation active after the switch. Zones added by hand and zones deleted by scripts can both create drift. Alert on the two directional metrics and on missing successful completions; a quiet drift graph without a recent completed run is not an acceptance signal.

## Roll back configuration without replaying business events

Before switching, assign a rollback owner and retain the approved previous hostname configuration. If the agreed mail acceptance criteria fail, stop advancing the cutover and have that owner restore the approved configuration through the normal change process. Reacquire the inventory and repeat the same delivery checks. Restoring a configuration is an action; verified recovery is a separate result.

Keep the reminder event identifier unchanged. A rollback must not become an instruction to send every appointment reminder again. If the scheduler retries the check, recompute the inventory counts without mutating DNS; if the sender retries an event, apply its application-level deduplication policy. Those are separate retry domains.

For the change review, preserve the failed acceptance evidence alongside the restored configuration and the successful verification. The useful postmortem question is which signal stopped the cutover and whether the operator could recover without duplicate patient notifications. Neither a clean inventory nor a successful cron invocation answers that question alone.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Cloudflare DNS documentation: https://developers.cloudflare.com/dns/
- Amazon Route 53 developer guide: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- Google Cloud DNS documentation: https://cloud.google.com/dns/docs
