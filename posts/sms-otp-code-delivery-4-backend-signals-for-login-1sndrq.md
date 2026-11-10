# SMS OTP Code Delivery: 4 Backend Signals for Login Retry and Cooldown

For SMS OTP login and password reset, have the backend own the code expiry and template revision before it can send a message or schedule a retry. Start with one durable challenge record and a delivery job that references it. In this proposed B2B SaaS runbook, the page reads “reset challenges expiring before submission.” The on-call sees a growing queue, old pending jobs, and reset attempts rejected after their deadlines. These are illustrative operating conditions, not a reported incident.

TL;DR: For SMS OTP login or password reset, persist the challenge before sending, enforce expiry and one-time consumption during verification, and apply separate limits to requests, guesses, and resends. Pin the template revision to that challenge. Alert on remaining usable lifetime before users encounter an expired code. A delivery retry must reuse the logical send identity; it must not silently create another valid code.

The same send-and-verify boundary matters when a login protects access to airline disruption alerts. A delayed flight notification and an expired authentication code have different consequences, so give authentication its own queue and deadline policy. The concrete example below stays with a SaaS password reset. Its primary decision is who controls the message users receive when queue delay starts consuming the validity window.

## What should the backend monitor before an SMS OTP login code retry?

Work backward from the rejection. Record the challenge creation time, fixed expiry, queue entry time, submission attempt, and available delivery outcome. Then inspect the template revision attached to the job. A successful submission alone cannot establish that a recipient received a usable reset message. Avoid letting a green transport counter close an authentication incident.

The earlier signal is deadline pressure.

For this design, use four signals: oldest pending challenge age, remaining lifetime at submission, challenges dropped before submission because they expired, and expired verification attempts. Keep the last metric separate from wrong-code attempts. They suggest different investigations. An expired attempt may justify inspecting delivery latency; a burst of wrong guesses belongs in the abuse investigation. Neither observation proves the cause by itself, and the runbook should say so.

Use a hypothetical 5-minute validity window to make the arithmetic concrete. A job waiting 4 minutes has at most 1 minute left before verification must reject it, even if the transport accepts it immediately. These are example policy values, not measured latency or a standards requirement. Choose the actual deadline from the recovery threat model and the delivery behavior measured in your own environment.

Do not restart the clock on retry. That hides queue delay while extending the period in which the credential works.

## Template ownership determines what the deadline means

Application-owned templates put the wording, locale, and revision alongside the challenge policy. That makes a statement such as “this code expires in 5 minutes” reviewable with the code that sets expiry. It also gives the application team responsibility for rendering tests and language changes. For queued messages, prefer an explicit expiry statement or carefully reviewed wording that does not imply the full validity period starts on receipt. The trade-off is release responsibility: application-owned rendering is not suitable for a team that cannot maintain approved translations and test them against the expiry policy. Delegated rendering is an alternative when a separate messaging team owns that review, provided the application still controls verification state and pins an approved revision.

Delegating rendering can reduce the application team's publishing work, but it adds a coordination boundary: someone must ensure that the deployed wording still matches the verifier's deadline. Decide who can edit a template, how a revision becomes approved, and which revision a queued job uses. A retry should retain the pinned revision unless an explicit policy invalidates and replaces the challenge. Otherwise an incident can leave two messages describing one credential differently, with no clear audit trail for the discrepancy.

The useful selection criterion is whether the team can reproduce the message for a specific challenge without recovering the secret from logs. Store the revision, locale, channel, and policy identifier. Keep the code and reset link out of observability data. For an email fallback, domain authentication is another independent concern: DMARC describes domain alignment and policy handling; it does not establish that a reset credential remains valid.

Own the contract.

## Put the remaining lifetime into the worker's decision

The instrumentation change should sit at the point where the worker decides whether submission is still useful. The following Go example is a complete policy function. Its inputs come from the persisted job and the worker's clock. The second argument is an explicitly chosen minimum remaining lifetime; it is not a transport timeout or a guarantee of delivery.

```go
package reset

import "time"

type Challenge struct {
	ExpiresAt        time.Time
	TemplateRevision string
}

type Decision struct {
	Submit    bool
	Remaining time.Duration
	Reason    string
}

func Assess(c Challenge, now time.Time, minimumRemaining time.Duration) Decision {
	remaining := c.ExpiresAt.Sub(now)
	if minimumRemaining < 0 {
		return Decision{false, remaining, "invalid_policy"}
	}
	if c.TemplateRevision == "" {
		return Decision{false, remaining, "missing_template_revision"}
	}
	if remaining <= 0 {
		return Decision{false, remaining, "expired"}
	}
	if remaining < minimumRemaining {
		return Decision{false, remaining, "insufficient_lifetime"}
	}
	return Decision{true, remaining, "ready"}
}
```

Record the returned remaining lifetime as a distribution and count decisions by reason. Export queue age separately so operators can distinguish backlog from a newly created challenge with an unusually short policy. Keep metric labels bounded: channel and a small reason set are useful; recipient identifiers, tokens, and arbitrary template revisions are poor aggregation keys. Put revision details in restricted structured events when needed for investigation.

This function does not coordinate concurrent workers. The surrounding worker needs an atomic claim, a persisted logical send identifier, and reconciliation for an uncertain submission outcome. Rechecking the deadline immediately before submission narrows the gap between assessment and action.

Submission isn't delivery.

That is a limitation of the local check: it cannot make transport delivery and database state one transaction.

That boundary deserves a named runbook branch. If a worker times out after submitting, record the attempt as uncertain and reconcile the available status before deciding on a retry. A local idempotency key prevents duplicate local work only to the extent that its storage and concurrency controls enforce it. Downstream duplicate suppression requires a separately established contract. Repeated messages should still refer to one challenge with one expiry.

## Verification and cooldown need separate state

NIST's cited guidance requires rate limiting for short authentication secrets and one-time acceptance of a valid out-of-band secret. It also treats use of the public telephone network for out-of-band verification as restricted. That makes SMS a recovery design decision requiring explicit assessment, rather than proof that possession of a phone number alone is sufficient for every account.

For this application design, generate the code with a cryptographically secure random source and keep verification material protected at rest. Verify the candidate against the stored challenge, check the fixed expiry, enforce the attempt budget, and consume a successful challenge atomically. Two concurrent valid submissions must not both authorize a password change. Bind the resulting reset authorization to the intended account and operation; a login challenge should not automatically become a password-reset credential.

Resend cooldown is a delivery control. Guess limiting is a verification control.

Apply request limits across the dimensions justified by the threat model, including the account and the requesting source, without treating a shared network address as a unique user. Keep account-existence responses consistent. On resend, either reuse the current challenge under an explicit policy or invalidate it and create a replacement atomically. Cooldown must not reset the failed-guess budget. Transport retry must not bypass either budget. These rules belong in shared state, so adding another backend process does not multiply the allowance.

Test the awkward transitions before release: verification exactly at expiry, two workers claiming one job, simultaneous successful verification attempts, and a resend racing with consumption. Add a template test that compares the rendered expiry wording with the configured policy. During deployment, preserve support for queued jobs referencing an older approved revision, or deliberately invalidate those jobs through a documented migration. Rendering and verification need a compatible rollout even when different teams own them.

## Tune the page against usable lifetime

Close the incident by measuring whether users had enough time to act, then trace the loss to queueing, submission, or later delivery evidence. Start remediation with the affected stage. Extra retries can increase delivery work and user confusion while leaving the original deadline pressure unchanged. Treat delivery spend as an operational constraint, alongside abuse exposure and on-call load; it is not the criterion for choosing who owns templates.

For an initial alert policy, require both sustained low remaining lifetime and a meaningful number of affected challenges. Keep a separate signal for a growing expired-before-submission count. Low-volume services need an explicit alternative because a percentage over a handful of requests is unstable. Pick thresholds from observed traffic and the service objective, and document what action each page authorizes. A warning without an operator action belongs on a dashboard until the response is defined.

The false-positive cost is concrete: a threshold set too close to normal queue variation can wake an operator for recoverable bursts, encourage unnecessary resends, and train the team to ignore deadline alerts. A threshold set too late leaves recipients holding expired credentials.

Review both outcomes.

The operational target is enough remaining lifetime for the reset flow to work, with a reproducible template and one enforceable verification deadline.

## References

NIST SP 800-63B supports the discussion of out-of-band authentication, secret acceptance, rate limiting, and telephone-network restrictions. RFC 7489 supports the narrower email-domain authentication discussion. The queue policy, example durations, instrumentation, and Go function above are proposed design choices, not claims that either document prescribes this architecture.

## Further reading

- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
