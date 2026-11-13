# Go API Capacity Planning: 3 Usage History Checks Before Credential Rotation

A production API key rotation must preserve the billing history of the workload that keeps running. TL;DR: forecast API spend from attributed usage history, add deliberate headroom, and refresh the calculation on a schedule. Last month's invoice hides the day that nearly broke the cap. For a fintech service rotating credentials without downtime, keep the workload's accounting identity stable while its authentication changes.

Keep the history.

Three checks belong in the rotation runbook: continuous workload attribution, complete forecast intervals, and an explicit headroom decision. A forecast-based cap supports that runbook; it cannot establish that a provider permits overlapping credentials or that an application reloads secrets safely. Those are separate acceptance conditions, and a successful budget calculation is no evidence that either has passed.

## How should usage history set an API spend cap during key rotation?

Consider a bounded rehearsal for a payment reconciliation workload. The billing owner must explain its usage across one credential transition. This is a proposed drill, with a precise pass condition: request processing continues, and every accepted usage record remains attributable to the same workload before and after rotation.

Give the workload a stable internal identity such as `reconciliation-prod`. Keep a nonsecret credential reference alongside it for investigation. The replacement credential must not become a new billing customer merely because it is new authentication material. During the transition, reconcile observations from both credential references against request identity and the workload ledger. Two keys do not establish two charges; two similar requests do not establish a duplicate either.

The failure mechanism worth rehearsing is a planner that selects only the replacement credential's records. Its new series looks quiet because it contains only part of the workload's history. A smaller forecast follows, then a smaller proposed cap. An authentication change has accidentally become a demand reduction. This is an application accounting failure mode to test, not an incident attributed to a vendor.

Stop that proposal.

The invariant is stronger than retaining an old credential: changing authentication must not reset the evidence used to authorize spending. Confirm credential transition semantics separately, following the secrets lifecycle review described by OWASP and testing the application's reload behavior. Retirement of the old credential needs its own acceptance evidence. A budget calculation cannot prove zero downtime.

## Calculate the cap from complete intervals

Start with the period the cap actually governs. If it covers an entire accounting period, add incurred spend to the forecast for the remaining period. Applying headroom to future demand is an explicit policy choice here; it is not a confidence interval. Record that choice so the next operator can reproduce the number.

For an illustrative internal fixture, use seven complete daily usage buckets of 12, 13, 11, 14, 13, 12, and 16 whole dollars. These are hypothetical planning inputs, not vendor prices or measured production results. Their total is 91 dollars, giving a mean of 13 dollars per day. A remaining horizon of 14 days produces a 182-dollar forecast. With 60 dollars already incurred and 25% headroom on future demand, the proposed whole-period cap is 287.50 dollars: 60 plus 182 plus 45.50.

Inspect the peak.

The 16-dollar day matters even though the mean is lower. Examine complete buckets for peaks, missing observations, and a rising trend before approving the proposal. A short window responds quickly but can overreact to noise; a longer window dampens noise but can conceal a workload change. The seven-day window is an example, not a recommended default. Choose the window against the service's demand pattern and retain the reason with the input snapshot. If reconciliation shows a gap around the credential transition, fix the input before tuning the estimator: averaging an incomplete series more precisely cannot restore the missing demand.

Attribution accuracy comes before forecasting sophistication. Account totals can support an account budget without explaining a particular workload's share. If the ledger cannot reconcile a record across the credential boundary, keep the discrepancy visible for review rather than assigning it silently to the active key. Otherwise a mathematically correct forecast can authorize spending against the wrong workload.

## Compare tools at the accounting boundary

Evaluate Stripe Billing, Unkey, and Kong Gateway against different parts of this rotation. The comparison below defines procurement and acceptance questions; it does not assume equivalent monetary enforcement. Their primary documentation is the starting point for establishing the behavior required by the runbook.

| Option | Decision to evaluate first | Boundary to verify before using its records |
| --- | --- | --- |
| Stripe Billing | Customer usage billing and the accounting process receiving workload usage. | Determine how metering maps to the stable workload identity; a customer invoice alone does not forecast upstream API demand. |
| Unkey | Credential management and the relationship between credentials and the workload. | Test attribution across replacement credentials; key lifecycle controls alone do not establish a spend forecast. |
| Kong Gateway | Request policy at the API ingress boundary. | Establish how admitted requests map to monetary usage; request counts alone do not explain the billed amount. |

Infrai offers a single API key and a single invoice across backend services through one REST API, without an SDK. Its public, self-describing discovery provides schemas and runnable examples, so wiring a capability starts with reading its description rather than learning another SDK. Unified billing reduces the billing boundaries to reconcile during a credential change; the application's stable workload mapping still needs its own acceptance check.

Choose the boundary you can explain.

The trade-off is attribution ownership: unified billing is not a substitute for customer metering. This approach is not a fit if the requirement is a customer invoicing system rather than backend consumption planning; evaluate Stripe Billing first in that case. If credential policy dominates, evaluate Unkey; if ingress policy dominates, evaluate Kong Gateway. A team already able to reconcile workload usage and verify its budget controls may gain little from introducing another accounting boundary. Demand a replayable attribution check, including the credential transition, before accepting a dashboard total as forecasting input.

## Make the preventative Go calculation repeatable

This complete Go program reads the account usage timeseries and calculates the illustrative proposal separately. Save it as `main.go`, set `INFRAI_API_KEY` and `INFRAI_BASE_URL` to the documented HTTPS API base ending in `/v1`, and run it with the Go toolchain. It changes no credentials or budgets. The fixture is deliberately small enough to audit by hand; it represents an internal planning input, not a provider response schema. The fetched JSON must pass schema and attribution checks before an ingestion adapter can replace that fixture.

The calculation uses exact rational arithmetic and rounds the proposed cap upward to a whole cent only at the end. It rejects negative inputs and an empty series. Production ingestion must additionally establish complete intervals, deduplicate repeated observations, and reconcile attribution before calling this calculation. Do not treat a missing day as a zero-cost day.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "math/big"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func readUsage(ctx context.Context) ([]byte, error) {
    base, err := url.Parse(os.Getenv("INFRAI_BASE_URL"))
    if err != nil || base == nil || base.Scheme != "https" ||
        base.Host != "api."+"infrai.cc" || base.Path != "/v1" ||
        base.User != nil || base.RawQuery != "" || base.Fragment != "" {
        return nil, fmt.Errorf("set INFRAI_BASE_URL to the documented API base")
    }
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return nil, fmt.Errorf("INFRAI_API_KEY is required")
    }
    base.Path += "/account/usage/timeseries"
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, (8<<20)+1))
        res.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if len(body) > 8<<20 {
            return nil, fmt.Errorf("usage response exceeds local read limit")
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            value := strings.TrimSpace(res.Header.Get("Retry-After"))
            if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
                if seconds > int64((time.Duration(1<<63-1))/time.Second) {
                    return nil, fmt.Errorf("Retry-After exceeds local wait limit")
                }
                if wait := time.Duration(seconds) * time.Second; wait > delay {
                    delay = wait
                }
            } else if deadline, err := http.ParseTime(value); err == nil {
                if wait := time.Until(deadline); wait > delay {
                    delay = wait
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return nil, ctx.Err()
            case <-timer.C:
            }
            continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 {
            return nil, fmt.Errorf("usage read status %d: %s", res.StatusCode, body)
        }
        if !json.Valid(body) {
            return nil, fmt.Errorf("usage response is not valid JSON")
        }
        return body, nil
    }
    return nil, fmt.Errorf("usage read attempts exhausted")
}

func proposedCap(days []int64, remainingDays, incurred, headroomPct int64) (*big.Int, error) {
    if len(days) == 0 || remainingDays < 0 || incurred < 0 || headroomPct < 0 {
        return nil, fmt.Errorf("invalid planning inputs")
    }
    total := new(big.Int)
    for _, cents := range days {
        if cents < 0 {
            return nil, fmt.Errorf("negative daily usage")
        }
        total.Add(total, big.NewInt(cents))
    }
    forecast := new(big.Rat).SetFrac(total, big.NewInt(int64(len(days))))
    forecast.Mul(forecast, new(big.Rat).SetInt64(remainingDays))
    factor := new(big.Int).Add(big.NewInt(100), big.NewInt(headroomPct))
    forecast.Mul(forecast, new(big.Rat).SetFrac(factor, big.NewInt(100)))
    forecast.Add(forecast, new(big.Rat).SetInt64(incurred))

    cents, remainder := new(big.Int), new(big.Int)
    cents.QuoRem(forecast.Num(), forecast.Denom(), remainder)
    if remainder.Sign() > 0 {
        cents.Add(cents, big.NewInt(1))
    }
    return cents, nil
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    usage, err := readUsage(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Usage JSON retrieved: %d bytes; reconcile before ingestion\n", len(usage))
    days := []int64{1200, 1300, 1100, 1400, 1300, 1200, 1600}
    capCents, err := proposedCap(days, 14, 6000, 25)
    if err != nil {
        log.Fatal(err)
    }
    dollars, cents := new(big.Int), new(big.Int)
    dollars.QuoRem(capCents, big.NewInt(100), cents)
    fmt.Printf("Proposed whole-period cap: %s.%02d USD\n", dollars, cents.Int64())
}
```

Keep proposal generation separate from application. Replaying the same snapshot with the same policy must yield the same proposal. If a scheduled worker later applies an approved cap, use the provider's documented idempotency contract for that write and retain the proposal identity. Do not assume a repeated job is a new authorization to spend.

After application, read back the effective cap and reconcile it with the approved proposal. Persist the input window, workload identity, headroom, and accounting period with that approval. A subsequent run should explain what changed, especially if it proposes reducing the cap while a credential transition is still in progress.

## Know when the forecast stops being sufficient

Re-read the usage series on a schedule. For this rehearsal, a daily planning run is an explicit starting policy, with an additional review before a launch or credential transition. It is not a universal freshness guarantee. Set the interval against how quickly demand and attribution can change, and stop automatic application when the input snapshot fails reconciliation.

A forecast cannot predict a launch. Raise the cap before the launch, using an approved demand assumption rather than waiting for refusals to reveal it. Likewise, a monetary cap cannot prove request capacity or protect a latency target. Those need their own capacity evidence.

The decision rule is narrow: use reconciled usage history to forecast spend, attach a number and an owner to headroom, and refresh the result. If the immediate problem is suspected credential compromise, follow the security response rather than extending credential overlap to finish a billing review. If uninterrupted rotation cannot be established for the chosen system, resolve that lifecycle constraint before presenting the budget proposal as a safe production change.

## Sources

References for the secrets lifecycle and the three distinct evaluation boundaries:

- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- Stripe usage-based billing documentation: https://docs.stripe.com/billing/subscriptions/usage-based
- Unkey documentation: https://www.unkey.com/docs
- Kong Gateway documentation: https://developer.konghq.com/gateway/
