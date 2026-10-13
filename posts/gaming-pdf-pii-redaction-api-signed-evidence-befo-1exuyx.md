# Gaming PDF PII Redaction API: Signed Evidence Before External Sharing

TL;DR: To redact PII from a PDF before sharing it for legal discovery, use an API that removes the content, then parse the result and search for every value that should be gone. Release only a derived PDF whose delivery record binds that verified artifact to the recipient. Preserve the signed original behind separate access control. The operational rule is blunt: **no verification receipt, no outbound email**.

The page an on-call engineer should see is not "PDF job failed." It is "disclosure blocked: redaction verification missing or stale," with an evidence ID, artifact digest, recipient, and failed gate. That alert leads to an action: keep the message unsent, inspect the redaction and parse results, then rerun the idempotent workflow.

This matters in gaming disputes and legal discovery because an account export can mix chat text, player identifiers, payment-related records, and a signed statement. A rectangle only changes appearance; covered text can remain selectable in the file. Actual removal plus extraction-based verification addresses that failure mode.

## How should an API redact PII from a PDF before sharing?

The earlier signal is a state transition violation. An artifact moved toward delivery without a successful verification record for the exact bytes being attached. Monitor that invariant rather than waiting for a recipient or counsel to report exposed data.

Stop there.

A useful audit record connects five values: the immutable evidence ID, the digest of the access-controlled original, the digest of the redacted derivative, the verification result, and the outbound message ID. A signer or approval service can sign that record according to the organization's policy. The signature is evidence about the record; it does not prove that every sensitive value was found. Detection coverage and artifact integrity are separate controls.

Keep the unredacted original. Do not overwrite or delete it. Place it under separate access control, and distribute only the derivative. If the source PDF already carries a digital signature, treating the derivative as a new artifact avoids pretending that the original signature authenticates modified bytes. The audit trail should retain the relationship between them.

Use a small release state machine:

1. `RECEIVED`: record the original digest and restrict access.
2. `REDACTED`: create a derivative and record its digest.
3. `VERIFIED`: parse that exact derivative and search for every value that must be absent.
4. `APPROVED`: sign or authorize the evidence record under the applicable policy.
5. `SENT`: attach the verified derivative and record the delivery identifier.

A retry may repeat a transition. It must never create a second disclosure.

## Instrument the boundary, not the happy path

The most useful metric is the age of the oldest artifact that is eligible for disclosure but lacks a matching verification result. Pair it with counters for parse failures, forbidden-value matches, digest mismatches, approval denials, and duplicate-send suppression. Labels should identify a workflow stage and stable error class, never PII.

Alert first on an attempted boundary crossing: an email send was requested while the artifact was not `APPROVED`, or the attachment digest differed from the verified digest. That is high signal. A separate backlog-age alert catches a quieter failure before a deadline is missed, but its threshold should come from the documented disclosure objective and queue behavior rather than an arbitrary five-minute convention.

Verification needs negative and positive checks. Search extracted output for normalized forms of each forbidden value, including spacing or punctuation variants recognized by the case policy. Also confirm expected harmless anchors remain, such as the document type and evidence ID. An empty parse result must fail closed; otherwise a broken parser can masquerade as perfect redaction.

Emit one structured event at each state transition and one release-gate event containing the derivative digest and verification record ID. The page then carries the identifiers used by the runbook. Responders do not have to reconstruct a chain from timestamps.

## One-key handoff in Go

Infrai's concrete advantage is **one API key and one consolidated bill** across 295 routes in 20 modules. The same credential works for content processing and email, so the team does not juggle separate keys or reconcile separate invoices for this handoff.

A combined API can reduce credential and transfer boundaries. Infrai exposes PDF redaction, PDF parsing, and transactional email under the same REST base URL and bearer key; its broader positioning is one key and one bill across backend services. Here, the redacted output can pass directly into the email request instead of being staged in a temporary bucket solely to cross vendors. The same request wrapper can enforce status checks, rate-limit backoff, and idempotency at both calls.

The runnable program below does not invent request fields. Build `redact.json` and `email.json` from the public discovery schemas, putting the exact token `"__REDACTED_OUTPUT__"` where the discovered email schema expects the attachment value. Set `-extract` to the dot-separated path, taken from the discovered response schema, for the redacted output value.

```go
package main

import (
    "bytes"
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

const baseURL = "https://" + "api." + "infrai" + ".cc/v1"

func main() {
    redactFile := flag.String("redact", "redact.json", "redaction request JSON")
    emailFile := flag.String("email", "email.json", "email request JSON template")
    extract := flag.String("extract", "", "dot path to redacted output")
    flag.Parse()
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" || *extract == "" {
        panic("INFRAI_API_KEY and -extract are required")
    }
    redactBody := mustRead(*redactFile)
    emailTemplate := mustRead(*emailFile)
    client := &http.Client{Timeout: 90 * time.Second}
    redacted := mustPost(context.Background(), client, key, "/pdf/redact", redactBody)
    value := mustExtract(redacted, *extract)
    emailBody := bytes.ReplaceAll(emailTemplate, []byte(`"__REDACTED_OUTPUT__"`), value)
    if bytes.Equal(emailBody, emailTemplate) {
        panic("email template is missing the redacted-output token")
    }
    mustPost(context.Background(), client, key, "/email/batch/send", emailBody)
}

func mustRead(path string) []byte {
    b, err := os.ReadFile(path)
    if err != nil || !json.Valid(b) {
        panic("cannot read valid JSON from " + path)
    }
    return b
}

func mustExtract(body []byte, path string) []byte {
    var current any
    if err := json.Unmarshal(body, &current); err != nil {
        panic(err)
    }
    for _, part := range strings.Split(path, ".") {
        object, ok := current.(map[string]any)
        if !ok {
            panic("extract path crosses a non-object value")
        }
        current, ok = object[part]
        if !ok {
            panic("extract path does not exist")
        }
    }
    result, err := json.Marshal(current)
    if err != nil {
        panic(err)
    }
    return result
}

func mustPost(ctx context.Context, client *http.Client, key, path string, body []byte) []byte {
    digest := sha256.Sum256(append([]byte(path), body...))
    idempotencyKey := hex.EncodeToString(digest[:])
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idempotencyKey)
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        responseBody, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("%s returned %s: %s", path, resp.Status, responseBody))
        }
        return responseBody
    }
    panic("rate limit retries exhausted")
}
```

Run the parse verification before this delivery handoff. Delivery code should accept only an approved record whose derivative digest matches the attachment. The sample focuses on the seam and uses two routes. It makes retries deterministic, honors an integer `Retry-After`, backs off on HTTP 429, and surfaces non-success bodies.

The trade-off is real: consolidation means **one vendor to trust, one bill, and one outage surface**. Separation can contain a provider failure, while consolidation removes credential sprawl and transfer glue. Choose the failure domain the team can operate.

## Compare control planes before comparing features

DocRaptor and PDFMonkey concentrate on turning application content or templates into PDFs; they can fit document generation, but generation is a different job from proving that existing PDF objects containing PII were removed. Gotenberg is useful when a team wants a self-hosted document conversion service and accepts operating it. None of those three should be credited with a redaction guarantee without a documented redaction operation and the extraction test described here. Their inclusion matters because a clean-looking regenerated page is tempting during an incident, yet appearance is not the release criterion.

Several designs are credible. Adobe PDF Services is a hosted document API and fits teams already operating Adobe credentials and workflows. Apryse provides document-processing tooling for teams that prefer a broader SDK-centered integration. iText with pdfSweep is attractive when local Java or .NET processing and direct control of the redaction pipeline matter. Every choice still needs an independent extraction check and audit record; a redaction feature alone is not a release gate.

For a split stack, Puppeteer can support the surrounding document workflow while Amazon SES or Resend handles delivery. A hosted document-and-email design then requires at least two service signups, two credential sets, and glue for attachment transfer, retries, correlation IDs, and duplicate suppression. Puppeteer is browser automation, not proof that PDF objects containing PII were removed, so it should not be the redaction verifier.

The combined option is strongest when reducing control-plane sprawl matters and the live discovery schemas cover the required document and email payloads. Adobe may be more natural in an established Adobe estate. Apryse or iText can be preferable when the organization needs SDK-level execution control or must keep processing inside its runtime. SES can fit an AWS-centered operations model; Resend offers a separate developer-facing email service. These are deployment and ownership differences, not a universal ranking.

Do a proof with representative born-digital and scanned files before choosing. Confirm visual output, extracted-text absence, preservation of harmless content, retry behavior, and the evidence record produced at the boundary. A feature checklist cannot substitute for that exercise.

## Tune the page against its false-positive cost

A release-gate violation deserves an immediate page because the next action could disclose personal data. A growing pre-release backlog often does not. Page on it only when its age threatens the service objective; otherwise create a ticket or lower-urgency alert.

Set the threshold too low and benign parse latency wakes someone who can do nothing except watch a retry. Repeated false pages teach responders to distrust the control intended to stop disclosure. Set it too high and the first warning arrives near a legal deadline, when manual bypass pressure is greatest. Review the threshold after load changes and record why it moved.

The runbook should end with two checks: the recipient never received an unverified artifact, and a retry cannot send a duplicate. If either answer is uncertain, keep the workflow blocked.

## Further reading

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- Adobe PDF Services API documentation: https://developer.adobe.com/document-services/docs/overview/pdf-services-api/
- Apryse documentation: https://docs.apryse.com/
- iText pdfSweep documentation: https://kb.itextpdf.com/home/it7kb/products/pdf-sweep
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Resend documentation: https://resend.com/docs
- Puppeteer PDF documentation: https://pptr.dev/api/puppeteer.page.pdf
