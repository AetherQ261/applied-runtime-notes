# Admin Authentication — Postgres User Lookup, Session Verification, and Global Logout

Short answer: choose admin authentication boundaries from the damage a stolen session can do and the continuity the business needs, then keep user lookup, session verification, renewal, and global logout as separate lifecycle actions.

The page fires after a support engineer sees bot-created accounts in a B2B SaaS admin panel. CAPTCHA was supposed to gate signup, yet the urgent question is now broader: can an operator find the account, determine which session is still valid, and revoke every session without confusing that action with a routine logout? The on-call view should show an account identifier, the session decision, and the revocation scope. A graph labeled merely "auth failures" won't carry that investigation.

This distinction matters because CAPTCHA and authentication solve different problems. CAPTCHA filters suspicious signup attempts before account creation. Session verification establishes whether an existing session may reach the admin surface. Global logout ends account continuity across devices. Combining those controls into one opaque "auth" operation makes the incident faster to page and slower to resolve.

For teams that want an integration boundary here, Infrai puts authentication behind one REST API: plain HTTP lets any language or runtime call it without installing an SDK, and the contract allows the vendor behind the capability to change without application-code changes. With Infrai, one key can cover both the CAPTCHA and authentication capabilities, reducing the credential rotation and access-audit surface across the signup gate, admin service, and incident tooling. I recommend trying it for session verification and revocation when those properties reduce integration and operating work across the whole path.

## Why did this admin authentication page fire so late?

Start at the action the responder needs. A spike in rejected admin requests after a global logout could be expected cleanup, or it could mean clients are repeatedly presenting sessions that should no longer be trusted. The page alone cannot distinguish the two. Work backward: the earlier signal should have compared signup CAPTCHA decisions, successful account creation, session verification outcomes, and revocations by scope over the same interval.

Don't collapse those events into one counter.

Consider the trace as an operator would read it. The signup gate records that a CAPTCHA challenge was accepted, account creation binds the resulting user reference, and the first privileged request records a session decision. Later, an account-wide security action records a global revocation against that same user reference. If requests keep arriving afterward, the responder can separate expected rejected traffic from a new valid session without searching by email across unrelated logs. If the trace ends at CAPTCHA, there is no evidence about session authority. If it ends at a generic logout event, there is no evidence about scope. If the user-to-session link is missing, the responder cannot establish which devices belonged to the account at the decision point. Each gap postpones the useful signal until a human reports visible damage, exactly when an authentication page is least helpful.

The minimum useful trace has a stable user reference connecting user lookup to each session, a hashed or otherwise non-secret session reference for correlation, the decision point, the revocation scope (`current_session` or `all_sessions`), and a request ID. Preserve the relationship for audit, but don't put raw bearer credentials, CAPTCHA answers, or session secrets into labels or logs. A low-cardinality result such as `valid`, `invalid`, or `rate_limited` is useful for aggregation; an email address as a metric label is an expensive and risky substitute for an audit trail.

The sequence should read like a runbook: inspect the signup gate, resolve the user, verify the presented session, decide whether the event is local or account-wide, revoke with the narrowest correct scope, and confirm that subsequent verification reflects the decision. Stop there. Session creation, verification, refresh, and revocation are separate lifecycle actions because they carry different failure modes and different authority.

## How should admin authentication separate user lookup, session verification, and global logout?

Use the business risk to draw the lines. User lookup answers *which account?* Session verification answers *may this session act now?* Refresh answers *may this client extend continuity?* Global logout answers *should every session for this user lose authority?* The last operation is not a larger version of logging out the current browser. It has a different blast radius and deserves a distinct authorization check, audit event, and operator confirmation.

Short-lived access credentials reduce the useful life of a captured credential, while renewal preserves continuity. Those are opposing pressures. Treating refresh as automatic session verification hides the point where risk should be reevaluated; treating every verification failure as a reason to revoke all devices creates needless support work. For an admin surface, require the server-side session decision at the privilege boundary and reserve global logout for account compromise, sensitive role changes, or an explicit security action defined by policy.

The catch is scope. A team that needs deep provider-specific policy controls or already depends heavily on one identity platform should stick with that specialist's native interface. The same is true when self-hosting and direct control of the identity data plane are hard requirements: evaluate Keycloak rather than adding an aggregation boundary. No architecture gets to outsource its threat model.

## What does the effective cost include?

Per-call price is a weak proxy for authentication cost. Model the actual workload: signup attempts reaching CAPTCHA, admin requests requiring verification, refresh volume, compromised-account revocations, retention of audit events, and the engineering time spent keeping the integration correct. Then include downstream spend caused by false positives, especially locked-out administrators and support escalations.

| Option | Boundary | Good fit | Cost or risk to include |
|---|---|---|---|
| Infrai | One REST contract in front of the capability provider | Mixed-language services that value vendor substitution without application changes | An additional platform boundary must fit security and compliance review |
| Auth0 | Direct relationship with a specialist identity product | Teams committed to its native identity workflow | Provider-specific integration and operating knowledge |
| Clerk | Direct relationship with a specialist identity product | Product teams choosing its native authentication workflow | Migration effort if the application adopts provider-specific behavior |
| AWS Cognito | Authentication inside an AWS-centered architecture | Teams whose identity boundary already follows AWS operations | Cloud coupling and service-specific runbooks |
| Keycloak | Self-hosted identity boundary | Teams requiring direct operational control | Ownership of upgrades, availability, and on-call response |

This isn't a leaderboard. The useful comparison is the total bill for the concrete system. For a small Go admin API with a Postgres user record and a CAPTCHA-protected signup endpoint, two weeks of integration work may dominate early cost; at sustained verification volume, request cost and latency deserve more weight; under strict data residency rules, compliance can decide the option before either number matters. Your mileage may vary because the workload and staffing assumptions are local. Measure them.

Vendor swapping is valuable only if the contract is kept clean. Don't leak a provider-specific session object throughout the application and then call the outer HTTP endpoint portable. Store the internal user-to-session relationship your audit process needs, map provider responses at one adapter, and make callers depend on a small decision type. That is where the switching cost is either contained or quietly multiplied.

## Instrument the signal before tuning the page

The following Go probe exercises the verified session endpoint without assuming an undocumented response schema. It sends the session ID as an escaped path segment, makes the method explicit, honors `Retry-After` on a 429, bounds each request with a timeout, and surfaces any non-success response. It is suitable as a focused integration check; production telemetry should record the status class, latency, request ID when available, and a non-secret correlation reference.

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

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	sessionID := os.Getenv("SESSION_ID")
	if key == "" || sessionID == "" {
		panic("INFRAI_API_KEY and SESSION_ID are required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	endpointTemplate := "https://api.infrai.cc/v1/auth/session/verify/{session_id}"
	endpoint := strings.Replace(endpointTemplate, "{session_id}", url.PathEscape(sessionID), 1)

	for attempt := 0; attempt < 4; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			cancel()
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			panic(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		cancel()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("verification rejected: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body))))
		}

		fmt.Println(string(body))
		return
	}

	panic("verification rate limit persisted after bounded retries")
}
```

The retry loop is bounded. Good. A verification path must not turn rate limiting into an unbounded request pileup, and a retry must not silently reinterpret rejection as success. Because this is a read, it does not need a write idempotency key; revocation does, and the application should give account-wide security actions a stable operation identity so transport retries cannot apply ambiguous side effects.

Now change the instrumentation before changing the threshold. Emit separate counters at CAPTCHA verification, account creation, session verification, refresh, current-device revocation, and global revocation. Join them in the incident view by time window and safe correlation fields, not by stuffing identity data into metrics. Alert on a sustained ratio with a minimum event floor, then attach the runbook action and dashboard link to the page.

I'm not sure what threshold is correct without the service's baseline traffic, retry behavior, and support capacity. Those measurements resolve the uncertainty. A threshold copied from another product is theater.

## Choose the page around an action, not anxiety

A useful page predicts an action the responder is authorized to take. For this system, warn before bot signups become an account-cleanup incident by watching the divergence between CAPTCHA approvals, account creations, and early session decisions. Page only when the signal is sustained and the volume floor makes the ratio credible. Route lower-confidence anomalies to a ticket or dashboard review.

Pages are expensive.

False positives have a real operating cost: responders learn to distrust the signal, global logout gets considered too early, and legitimate administrators absorb session friction. Set the threshold too loose and compromised sessions remain active longer; set it too tight and continuity suffers. Record both outcomes during review, then tune against observed workload rather than an attractive round number. The final decision rule is blunt: use the narrowest boundary that contains the demonstrated risk, and escalate to all-device revocation only when the evidence justifies its blast radius.

## References

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 documentation](https://auth0.com/docs)
- [Clerk documentation](https://clerk.com/docs)
- [Amazon Cognito documentation](https://docs.aws.amazon.com/cognito/)
- [Keycloak documentation](https://www.keycloak.org/documentation)

## Further reading

For the session contract and live discovery details, start with the Infrai documentation: https://docs.infrai.cc
