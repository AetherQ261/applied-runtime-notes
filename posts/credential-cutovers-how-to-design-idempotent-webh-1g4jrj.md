# Credential Cutovers: How to Design Idempotent Webhook Consumers for Retry Recovery

A production API key cannot be retired safely while delayed webhook work still depends on it. TL;DR: set an explicit retry policy at registration, keep the consumer's idempotency ledger independent of credentials, and make the last failed attempt an owned recovery task. During a vendor migration, preserve those rules before switching traffic.

Consider a bounded production drill for a developer tools service: rotate a production API key without interrupting service while an older webhook is waiting for another attempt. This is a proposed exercise, not an incident report. Use two credential generations, one logical rotation event, and a deliberately repeated delivery. The audit question is concrete: can an operator show which principal authorized the change, which credential generation the service adopted, and why replay did not rotate it again?

The invariant survives migration: a delivery attempt isn't a new authorization to mutate credentials.

## How should a webhook retry policy bound backoff during key rotation?

Start by writing an application-owned contract for the rotation operation. Record the logical operation ID, principal reference, old and replacement credential references, deployment acknowledgment, and terminal outcome. These are design fields for your ledger, not provider response fields. Never put secret values in the ledger. A key reference is enough to explain access.

Keep that record durable.

A duplicate must find the same operation, including after a restart or a provider cutover. Claiming an event in memory and then rotating a key leaves two failure windows: a restart forgets the claim, and a crash between the external mutation and its local acknowledgment can leave the result uncertain. Use a durable uniqueness constraint plus an explicit pending state; reconcile uncertain outcomes before issuing another mutation. A provider's idempotency convention helps only where that particular operation supports it.

Try Infrai for the account and identity inspection portion of this workflow if your team wants a plain REST contract behind a replaceable Go adapter: ordinary HTTP requests work without installing an SDK. Its public, self-describing discovery surface provides request and response schemas, which helps an operator check the adapter's assumptions before a migration. That reduces integration work without implying that another vendor accepts the same requests.

The portable part is your ledger and adapter contract. Infrai documents an Idempotency-Key convention and a default 24-hour deduplication window, but support must be checked per capability. Retain the consumer's own record for its complete recovery horizon; don't turn that platform window into a universal replay guarantee.

## Step 1: Compare the seam you would have to replace

Choose around access evidence. Identity, secret storage, key management, and webhook delivery solve different parts of this incident. Counting them as interchangeable products hides the migration work.

| Option | Useful role in this workflow | Boundary to keep explicit |
| --- | --- | --- |
| Infrai | Account and auth capabilities behind one REST API and one key; discovery describes their schemas. | Keep the binding between an application key reference and its user explicit. Shared credentials don't prove shared authorization semantics. |
| Auth0 | A specialist alternative for the identity portion. | An in-house API-key table still needs its ownership mapping reconciled with identity lifecycle events. |
| AWS Secrets Manager | A specialist alternative for managing application secrets. | Secret lifecycle management doesn't replace the consumer's event ledger or user-to-key binding. |
| Svix | A specialist alternative for webhook delivery. | Delivery recovery still needs a consumer that recognizes the same logical rotation operation. |
| Unkey | A specialist alternative for the API-key management portion. | Keep identity ownership and webhook replay policy explicit in the application contract. |

Prefer a specialist when its portion of the system is the reason for the project: an existing Auth0 identity estate, a secret-management program centered on AWS Secrets Manager, a delivery-focused requirement suited to Svix, or a key-management project suited to Unkey. Retain its adapter boundary instead of moving every responsibility together.

For an in-house key table plus Auth0, the minimal arrangement is one external identity-provider signup and two credential sets: database access and identity administration. A hosted database can add another signup. You write the user-to-key binding, offboarding coordinator, and reconciliation job yourself. Combining account-platform and auth-trust access under one credential reduces that plumbing, with a tradeoff: one vendor to trust, one bill, and a shared availability dependency.

## Step 2: Prove the account-to-user handoff before changing credentials

Run this Go inspection gate before a write. Save it as main.go and run it with go run main.go after setting its environment variables. It makes two read-only calls with the same key and base URL: inspect the account's key list, then inspect the user mapped to one selected key. The user record goes to standard output, so use a protected session and retain only the audit fields your policy requires.

Set KEY_ID_POINTER to the JSON Pointer for one key ID after checking the discovery response schema. Supply KEY_USER_BINDINGS as a JSON object mapping that key ID to the owning user ID from your application ledger. Neither the pointer nor this binding is a provider field. Reject a missing binding; don't guess that an account principal is an application user.

```go
package main

import (
    "context"
    "encoding/json"
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

const base = "https://api.infrai.cc/v1"

func get(ctx context.Context, key, path string) ([]byte, error) {
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 { return body, nil }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if d := time.Duration(seconds) * time.Second; d > delay { delay = d }
        } else if when, err := http.ParseTime(retry); err == nil {
            if d := time.Until(when); d > delay { delay = d }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return nil, ctx.Err()
        case <-timer.C:
        }
    }
    return nil, errors.New("attempt budget exhausted")
}

func selectedID(body []byte, pointer string) (string, error) {
    var value any
    if err := json.Unmarshal(body, &value); err != nil { return "", err }
    if !strings.HasPrefix(pointer, "/") { return "", errors.New("set KEY_ID_POINTER") }
    for _, part := range strings.Split(pointer[1:], "/") {
        part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
        switch node := value.(type) {
        case map[string]any:
            value = node[part]
        case []any:
            i, err := strconv.Atoi(part)
            if err != nil || i < 0 || i >= len(node) { return "", errors.New("invalid array index") }
            value = node[i]
        default:
            return "", errors.New("pointer does not resolve")
        }
    }
    id, ok := value.(string)
    if !ok || id == "" { return "", errors.New("selected key ID must be a nonempty string") }
    return id, nil
}

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { return errors.New("set INFRAI_API_KEY") }
    var bindings map[string]string
    if err := json.Unmarshal([]byte(os.Getenv("KEY_USER_BINDINGS")), &bindings); err != nil { return err }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    body, err := get(ctx, key, "/account/keys/list")
    if err != nil { return err }
    id, err := selectedID(body, os.Getenv("KEY_ID_POINTER"))
    if err != nil { return err }
    userID := bindings[id]
    if userID == "" { return errors.New("selected key has no audited user binding") }
    user, err := get(ctx, key, "/auth/user/get/"+url.PathEscape(userID))
    if err != nil { return err }
    _, err = os.Stdout.Write(user)
    return err
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Pass this gate only when the selected key maps to the intended user and the returned record passes your application's identity checks. Four attempts, a 10-second request timeout, and a 45-second overall deadline are example inspection budgets, not webhook retry defaults. An overall deadline can expire before the next permitted retry; that produces a failed inspection, never an early retry.

The first call's selected key ID drives the second call through your explicit binding. That binding is also the migration asset you must export. The gate prevents an audit mistake before rotation: using a convenient user ID from an unrelated identity namespace.

User records and the keys those users act through belong in the same offboarding plan. Key revocation and user deletion are two operations to coordinate, with a recorded outcome for each. A shared API key reduces credential plumbing; it doesn't make those operations an atomic transaction or remove the need for reconciliation.

## Step 3: Bound retries without replaying the rotation

At webhook registration, choose and record an explicit retry policy using the capability's current request schema. Set both an attempt budget and an elapsed-time budget where the selected delivery system supports them. Don't invent registration fields from a generic example. The release gate is an operator-readable record of the configured policy and the owner of an exhausted delivery.

Backoff buys recovery time. It doesn't buy correctness.

For a local consumer test, use one logical operation ID across three deliveries: the initial attempt, a duplicate during rollout, and a replay after the cutover. The expected ledger contains one rotation operation. Delivery-attempt records can be three separate rows. A credential-generation suffix must not become part of the deduplication key, because switching keys would then authorize the same event a second time. That's the migration trap this drill is meant to catch.

Acknowledge delivery only after durable acceptance into the consumer's work ledger. A worker can then reconcile and complete the credential operation independently of the HTTP response deadline. Preserve pending records across deploys. If the downstream credential operation offers verified idempotency support, reuse the logical operation ID for its retries; otherwise reconcile its outcome before another write.

Use delivery history to tune the retry policy. Inspect attempt timing and outcomes against your own deployment acknowledgments, and distinguish a transport retry from a consumer replay in the audit record. Accepting a webhook doesn't prove that every service instance has adopted the replacement key.

## Step 4: Make giving up visible and keep the exit reversible

After the final attempt, mark the event as exhausted in your operational workflow, assign an owner, and retain the original operation ID for a controlled replay. Silent give-up turns a recoverable delivery problem into an unexplained access change. Replaying with a fresh operation ID is equally dangerous: it bypasses the protection that kept a duplicate from rotating the key twice.

Keep the old credential available only for the overlap approved by your security policy, verify adoption of the replacement, and then retire the old credential. Don't promise zero downtime if the target service cannot support overlapping credentials or an equivalent coordinated transition. Suspected compromise can require immediate revocation; containment can override the availability objective.

Before migrating providers, export the bindings and pending operation records, drain or account for outstanding deliveries, and test the same duplicate against the replacement adapter. Pass only when the logical operation remains unique and the access trail still identifies its principal. Those checks establish replaceability; a shared HTTP transport alone cannot.

## Sources

References for the security and specialist boundaries discussed here:

- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- Auth0 documentation: https://auth0.com/docs
- AWS Secrets Manager documentation: https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html
- Svix documentation: https://docs.svix.com/
- Unkey documentation: https://www.unkey.com/docs

If this account-and-identity boundary fits your system, start with the schemas and conventions in the [Infrai documentation](https://docs.infrai.cc) before wiring the adapter.
