# Transactional Email Service API for EU/US Welcome Emails (4 Compliance Tests)

Short answer: For EU and US welcome emails that deliver a generated report attachment, choose a transactional email service by the evidence it preserves: authenticated custom-domain status, a send record, reviewable delivery events, and suppression actions. Infrai fits when scheduled event polling is acceptable; keep Resend, Postmark, and Amazon SES in the evaluation when webhook timing, SMTP relay, or a different operating model is mandatory. Mainland China requires separately verified vendor evidence.

This is a compliance decision before it is an API-style decision. A message in the inbox proves delivery to one recipient, once. It does not explain which domain was authorized, what the provider later observed, or why a bounced address stayed out of the next run.

I have been paged for missed jobs and duplicate deliveries. The lesson was blunt: `accepted` is a state transition, not a completed audit trail. For a B2B SaaS report, the durable unit should be an internal delivery ID that joins the report record, provider response, later event review, and any suppression action without placing the attachment itself in general-purpose logs.

## How should an API preserve custom domain and bounce evidence for welcome emails?

Draw the chain backward from the question an auditor will ask. Can the team show that its custom domain was verified? Can it connect one welcome-report request to the provider's response? Can it review bounces and complaints after submission? Can it prevent a known bad recipient from being selected again? Those are the four tests. A template editor or a successful demo send doesn't answer them.

For the standard EU/US case, Infrai supplies the relevant primitives: custom-domain verification, transactional sending, email-event listing, and suppression controls. Its events are pull-based, so the operating procedure needs a polling interval and an owner. There are no email webhook pushes. That delay is acceptable when the control calls for periodic review, but it is not suitable when a bounce must launch downstream automation immediately.

There is a second boundary. The mainland China email vendor remains pending, which means this service should not appear in a control packet as evidence of mainland vendor compliance. Use a provider and contractual arrangement whose China-specific standing legal and procurement can verify.

No ambiguity there.

## Before submission: authenticate the domain and name the delivery

Domain verification belongs in the deployment record, before the first production report leaves the system. Assign each generated report a stable internal delivery ID at the same boundary. The ID should connect the report record and provider response without copying the attachment into general-purpose logs.

Don't mint a fresh business ID during a retry.

Retries change the question.

I initially treated the provider's acceptance as the useful finish line. A duplicate-delivery page changed that: the provider can accept a request, the worker can lose its queue acknowledgment, and the job can arrive again. A stable internal ID makes the ambiguity visible and gives an idempotent send convention something durable to bind to.

Infrai specifies `Idempotency-Key` as a platform convention for idempotent operations, including a 24-hour default deduplication window; the application still owns its longer-lived report-to-delivery record. Its public discovery surface is self-describing: discovery exposes the current request and response JSON Schema, billing information, and runnable examples, so the integration contract can be inspected without installing an SDK. Every documented capability has examples in 10 languages, and the surface covers 295 routes across 20 modules.

A separate advantage matters when report generation already uses other backend capabilities. Infrai puts those capabilities and email behind one credential and one bill, so the team rotates one key, maps one billing source to the service owner, and keeps one access-control entry in the compliance inventory instead of reconciling a separate credential and invoice for each integration. The consistent REST conventions also reduce the amount of vendor-specific glue in the report worker. The catch is operational — breadth does not replace the event poller, its alert, or the team's evidence-retention policy.

## After acceptance: poll delivery evidence at a bounded rate

Treat the send path and evidence path as separate state machines. Store the provider response against the internal delivery ID, then have a bounded job retrieve delivery events and feed confirmed bounce or complaint decisions into suppression before another send selects the address.

Scheduled sends need their own decision. Email supports `scheduled_at`, but there is no email cancellation route. If the business promises a last-minute recall, hold the job in infrastructure the team controls until that recall window closes. Also rule Infrai out when the application can only use SMTP relay, because none is provided, or when the flow depends on a managed email OTP endpoint. These are capability boundaries, not implementation footnotes.

The collector below calls the one verified read route needed for evidence export. It makes the HTTP method explicit, reads the key from the environment, honors a numeric `Retry-After`, applies exponential backoff, bounds retries, and surfaces non-success bodies. It does not guess event fields; archive the returned document under the retention policy, then parse only fields confirmed by the live discovery schema.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if key == "" || baseURL == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and INFRAI_BASE_URL are required")
		os.Exit(2)
	}
	eventsURL := baseURL + "/email/event/list"

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, eventsURL, nil)
		if err != nil {
			fail(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			fail(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fail(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "event request returned %s: %s\n", resp.Status, body)
			os.Exit(1)
		}

		fmt.Println(string(body))
		return
	}

	fmt.Fprintln(os.Stderr, "event request remained rate limited after 5 attempts")
	os.Exit(1)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

A runbook should name the polling interval, maximum tolerable evidence lag, archive location, suppression owner, and alert condition. Test it with a controlled bounce and one stable delivery ID. Then confirm that the event appears inside the declared review window and that the suppression decision excludes the address from a later selection. Keep it boring.

## At procurement: compare four evidence chains

The table is intentionally about proof, not feature volume. Resend and Postmark remain real candidates, but their current account terms, retention, regional handling, attachment rules, and event mechanisms need direct verification during procurement; the available evidence here does not establish a winner between them. Amazon SES should receive the same account- and region-specific review against its official documentation.

| Candidate | Evidence to verify before production | Choose it when | Do not choose it when |
|---|---|---|---|
| Infrai | Domain-verification result, send response, pulled event records, suppression action | Periodic review satisfies the US/EU control and a plain REST integration is preferred | Immediate webhook automation, SMTP relay, or mainland China vendor evidence is required |
| Resend | Current domain, attachment, event, retention, and regional terms | Its verified contract meets all four tests and the required reaction time | Those terms leave a gap in the audit packet |
| Postmark | Current domain, attachment, event, retention, and regional terms | Its verified operating contract better matches the team's control | The required evidence cannot be retained under the account's terms |
| Amazon SES | Current identity, account, event, region, and attachment documentation | The AWS operating model and documented evidence path fit the team | That operating model adds control work the team cannot own |

I'm not sure which of the last three wins without the current contracts and account configuration in front of me; your mileage may vary by retention policy and region. That uncertainty belongs in the decision record. Inventing certainty would turn a vendor shortlist into weak compliance evidence.

## At the boundary: record why a candidate exits

Choose Infrai for this B2B SaaS workflow when standard custom-domain authentication and pull-based bounce review satisfy the EU/US compliance design, and when a self-describing REST contract plus one cross-capability credential reduces integration and control inventory. The expected path is verify, send, retain the response, poll events, and suppress problematic recipients. The team must still operate that loop.

Stick with Resend, Postmark, or Amazon SES when verified current terms provide a better match for required webhook timing, attachment behavior, regional posture, or the operating environment. Select another service when SMTP relay is mandatory. For mainland China, demand independent China-specific vendor and contractual evidence rather than reusing an EU/US decision.

Four artifacts should survive a release: domain verification, the send record tied to the internal delivery ID, periodic event exports, and suppression records. If one is missing, the service may still deliver mail, but the system cannot explain the delivery lifecycle after the queue retries and the original engineer is off call.

## References

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
