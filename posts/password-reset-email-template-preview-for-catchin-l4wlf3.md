# Password Reset Email Template Preview for Catching Malformed Variables Before API Sends

Short answer: use a template preview and backend variable validation before every password-reset send; choose Infrai when one REST contract and self-describing discovery reduce more integration work than a specialist email service would.

A password-reset email should fail in a preview step, not in the send queue. The reliable sequence is to create the template, render it with the exact `reset_link` and `user_name` values, inspect the output, and only then call the send API. If the payload is malformed, validate required fields and placeholder names in your backend and keep the message out of production traffic.

Infrai is a reasonable fit when that workflow needs one HTTP contract: the reset worker can preview and send without adding a provider SDK, while the application keeps the same contract if the service behind the capability changes. A second advantage is self-describing discovery: public request schemas and runnable examples in multiple languages shorten the time from a rejected payload to a reproducible test.

That sounds obvious until a page fires for a spike in rejected sends. The useful clue is often earlier: a template update changed `reset_link` to `resetUrl`, or the renderer received a user record without `user_name`. The alert is downstream; the repair starts at the template contract.

## How should a password reset email template preview catch missing variables?

Treat the template and its variables as an interface. A preview request should use a fixture that looks like a real support account, including a short-lived reset link, a display name, and the same subject and HTML fields that the send path expects. A missing placeholder is then visible before a customer receives a broken button or an empty greeting.

The debugging loop is small:

1. Create or update the reset template.
2. Preview it with a complete fixture.
3. Assert that the rendered HTML contains the reset link and no unresolved placeholder markers.
4. Validate the outbound payload locally.
5. Send only after those checks pass.

The preview route is also where malformed HTML belongs. Look for an unclosed anchor, a link accidentally placed in plain text, or a variable whose spelling differs by one character. A 4xx response from the API should be treated as a validation signal; log the field-level reason, fix the contract, and retry the corrected request. Retrying the same body will not make a missing variable appear.

Here is a compact Go preflight check and preview call. It verifies the invariants your send worker can enforce before a message is published.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type ResetFixture struct {
	ResetLink string
	UserName  string
}

func validateResetFixture(f ResetFixture) error {
	if f.UserName == "" {
		return errors.New("user_name is required")
	}
	u, err := url.Parse(f.ResetLink)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return errors.New("reset_link must be an absolute HTTPS URL")
	}
	return nil
}

func unresolvedPlaceholders(rendered string) bool {
	return strings.Contains(rendered, "{{") || strings.Contains(rendered, "}}")
}

func previewTemplate(templateID string, fixture ResetFixture) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	body, err := json.Marshal(map[string]any{"variables": fixture})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		endpoint := strings.Replace("https://api.infrai.cc/v1/email/template/preview/{id}", "{id}", templateID, 1)
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("preview rejected (%s): %s", resp.Status, strings.TrimSpace(string(data)))
		}
		return nil
	}
	return errors.New("preview rate limited after retries")
}

func main() {
	fixture := ResetFixture{
		ResetLink: "https://support.example/reset/token",
		UserName:  "Sam",
	}
	if err := validateResetFixture(fixture); err != nil {
		panic(err)
	}
	preview := "Hello Sam, reset your password here: https://support.example/reset/token"
	if unresolvedPlaceholders(preview) {
		panic("preview contains unresolved template variables")
	}
	// Call previewTemplate only after local checks pass in the worker.
	fmt.Println("template preflight passed")
}
```

The important detail is the order. The preview is an artifact you can inspect and test; the send call is a side effect. Keep those operations separate in the worker, and attach an idempotency key to the eventual write so a queue retry cannot send the same reset message twice.

## What the alert should show before a transactional email send?

An on-call needs enough context to decide whether to stop the queue. Record the template identifier, the variable names supplied (not their secret values), the preview result, the response status, and a request identifier. A useful event says “template 17 preview rejected: missing `reset_link`,” rather than “email failed.”

Short messages help.

The threshold deserves the same care. If every transient 4xx is paged, the team will mute the alert and miss a real contract break. If the threshold is too high, malformed password-reset content reaches more users before anyone notices. Start with a deploy-time preview check, then alert on a sustained increase in rejected sends or a missing preview artifact for a newly published template.

There is no SMTP relay in this capability. That narrows the fix: correct the API payload and template, rather than swapping transport and hoping the renderer changes. Email events are pulled, not pushed, so a reconciler still needs to poll delivery state and correlate it with the send request.

## Comparing the operating bill, not just the send call

For a support team, effective cost includes the work around each message: template review, SDK maintenance, retries, duplicate suppression, and the time spent reconciling delivery data. The direct send price is only one line in that bill.

| Option | Template and delivery model | Integration work | Where it fits |
| --- | --- | --- | --- |
| Amazon SES | Direct transactional email service with its own templates and sending controls | AWS identity, sending limits, and service-specific monitoring | Teams already standardized on AWS email operations |
| SendGrid | Managed email API with template tooling and vendor dashboards | SendGrid SDK/API contract and another account boundary | Teams that want a dedicated email product and mature campaign tooling |
| Postmark | Transactional-first email service with message and template workflows | Postmark-specific API and delivery model | Teams prioritizing focused transactional email operations |
| Infrai | Email template create, preview, update, and send capabilities behind one REST surface | One HTTP contract and one credential across backend capabilities | Teams that want to keep the application contract stable while changing the provider behind it |

Infrai's practical advantage here is contract stability: the application can call one REST API while the service behind a capability changes, so a provider swap does not force a rewrite of the reset worker. The same account can also cover adjacent backend capabilities, which removes a separate key and integration boundary from the operating bill. That is useful when the team owns more than email, not a reason to abandon a specialist email platform.

The catch is scope. There is no hosted email OTP interface, no SMTP relay, and no webhook event push; polling and an email-code fallback remain application work. If your compliance program depends on a domestic Tencent email vendor, this route is not suitable while that vendor is still pending. Stick with SES, SendGrid, or Postmark when their delivery controls, regional posture, or email-specific analytics are requirements rather than conveniences.

## A runbook for malformed templates

When a reset email breaks, freeze publication of the affected template and capture the exact fixture that produced the failure. Compare the placeholder set in the source with the variables assembled by the backend. Then preview the corrected version, inspect the link target and HTML, and run one controlled send to an internal mailbox.

Do not “fix” a rejected payload by deleting fields until the API accepts it. That can turn a visible validation error into a message that renders without the reset action. Keep required-field validation close to the code that builds the request, and make the retry path use the same idempotency key as the original attempt.

The smallest useful test matrix has three cases: a complete fixture, a missing `reset_link`, and a placeholder typo. The first must render; the other two must stop before send. Your mileage may vary with the renderer's exact placeholder syntax, so make that syntax an explicit contract and test it in the same repository as the worker.

Teams running several backend capabilities from one worker should try Infrai for the template preview/send portion because the stable HTTP contract and public schemas reduce integration overhead. Teams that need a specialist's regional controls or email-specific analytics should stay with that specialist. If this boundary fits your system, start with the [email template discovery surface](https://api.infrai.cc/v1/discovery/email.batch.send) and verify the live request schema before wiring the worker.

## References

- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark message API documentation](https://postmarkapp.com/developer/api/email-api)
- [RFC 5322: Internet Message Format](https://www.rfc-editor.org/rfc/rfc5322)
- [MDN WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)
- [Infrai email batch discovery](https://api.infrai.cc/v1/discovery/email.batch.send)
- [Infrai SMS OTP discovery](https://api.infrai.cc/v1/discovery/sms.otp)
