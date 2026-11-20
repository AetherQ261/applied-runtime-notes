# Internal DNS Infrastructure Code: A Go Guide to Logistics Cutover Gates

Manage internal DNS hostnames with infrastructure code while moving logistics zones off a registrar-specific API. The least complex starting point is a reviewed record set in the infrastructure repository, applied with upsert during deployment.

The page to design around is concrete: a dispatch client still resolves the previous destination while the deployment reports success. That is a hypothetical failure condition, but it identifies exactly what the on-call needs to see: the expected record, the observed answer, and the deployment revision that changed it.

TL;DR: Apply the intended internal DNS records on deploy, read them back, and fail the deployment on a diff. Treat propagation as a separate release gate. A matching provider response establishes configuration agreement; it does not establish that the resolver used by a warehouse client has the new answer. Keep rapidly changing names in a service registry.

This note uses an illustrative logistics change: move a stable dispatch hostname and the DNS ownership workflow for shipment email away from registrar-specific automation. The decision is propagation delay versus cutover speed. The useful optimization is shortening the interval between a bad assumption and a failed gate, while preserving enough overlap to keep dispatch reachable.

## How should infrastructure code manage internal DNS hostnames before a cutover?

A deployment should first detect disagreement between desired records and the provider's read-back result.

That is the earlier signal.

If an out-of-band edit changes an internal name, the repository comparison should expose it as a failed deployment, with the record identity and revision attached. Hand edits otherwise accumulate into configuration that nobody can explain six months later.

There are two observations to retain. One is the configuration returned by the DNS management interface. The other is the answer returned along the client's resolver path. Combining them into a single green indicator loses the distinction that matters during a cutover: the controller may have accepted the intended record while a client still receives an earlier answer. Repeating the write in response to that second condition adds activity without proving that propagation has advanced.

Keep both observations.

For example, use `dispatch.warehouse.example.com` as the stable internal hostname and `mail.shipments.example.com` as the email domain in a rehearsal dataset. These names are illustrative, not claims about a deployed system. Keep their ownership boundaries separate in the repository, even when one controller handles both. A worker moving between hosts every few seconds is a different naming problem; do not turn this deployment gate into a substitute for service discovery.

Record the expected answer, actual answer, resolver identity, observation time, and deployment revision as local instrumentation fields. These are proposed controller fields, not claims about a vendor's response schema. They make the later incident question answerable: did the intended change fail to apply, or did the client retain an older view?

## Fast writes do not define a safe cutover

The controller's sequence is straightforward: review the desired record set, apply upserts, read back, and compare the managed records. A mismatch blocks progression. Scope the comparison to the records the controller owns; otherwise a legitimate record managed by a different team becomes indistinguishable from drift. Conversely, silently ignoring a changed owned record defeats the reason for putting DNS in the repository.

Upsert makes repeated reconciliation useful, but it isn't permission to replay every write without an identity. Give a mutation a stable identity derived from the intended change, preserve it across retries, and follow the provider's documented idempotency rules. A new attempt identifier for every retry can describe multiple operations when the intent was one.

Read-only probes have a simpler retry story.

After configuration agreement, hold the old destination through an explicitly chosen propagation window and probe from the resolver path that matters to dispatch. The window belongs in the change plan. There is no universal safe number in this note: the record settings and observed resolver behavior must justify it. A faster API response does not justify retiring the old target sooner.

That costs overlap capacity. It also makes rollback less dependent on another DNS change becoming visible immediately. Keep the old target usable until the release gate permits retirement; changing the repository back is a desired-state action, not proof that all clients have already returned.

Don't skip the overlap.

## Compare the DNS and email ownership boundary

Moving off a registrar-specific API does not require moving every backend service at once. Compare the following real alternatives against the same acceptance test: can the team review the intended records, apply them safely, read them back, and carry the verified domain identity into the email check?

| Option | Operational fit | Boundary to account for |
| --- | --- | --- |
| Amazon Route 53 with Amazon SES | Consider this when DNS and shipment email belong in an AWS account and its access model. | DNS state and email domain identity still require explicit reconciliation; a shared cloud account does not establish resolver convergence. |
| Cloudflare DNS with Resend | Consider this when the team wants DNS and email to remain separate product choices. | Maintain the handoff between DNS records and email domain verification, including the recheck after a signing-key rotation. |
| Existing registrar DNS with a repository controller | Consider this when replacing hand edits is more urgent than replacing the current provider. | The controller still depends on the registrar-specific API that the logistics migration is intended to remove. |

Infrai is another option when the team wants DNS and email behind one API key and one bill, reducing credential distribution and month-end invoice reconciliation across backend services. Its public discovery surface supplies request and response schemas, which helps implement the DNS-to-email handoff without guessing fields. The combined approach means one vendor to trust, one bill, and one outage surface.

The trade-off is shared operational dependency. This approach is not a fit when policy requires DNS and shipment email to have independent provider ownership; choose a split stack such as Cloudflare with Resend in that case. An existing AWS access model can instead favor Route 53 with SES. None of these choices removes the propagation gate.

Count the alternative stack honestly. Cloudflare plus Resend means 2 product signups, 2 product credential sets, and glue that reconciles mail-related DNS records with the email domain state. Route 53 plus SES can share 1 AWS signup and an authorized AWS credential set; it still needs permissions and reconciliation across 2 services. Separate deployment roles may be appropriate. Calling both arrangements “two keys” would hide a meaningful difference.

For shipment email, the handoff is more than copying SPF or DKIM values between dashboards once. The controller needs to recheck the records and the email domain after a DKIM rotation. A DNS read-back confirms what was configured. The email domain check supplies a second observation, and the release policy decides whether those observations authorize sending.

## A Go probe that carries the domain into the email check

The following is a focused, runnable observation probe. It reads the DNS record list, extracts a domain string with a configured JSON Pointer, and uses that output in the email domain lookup. Both requests use the same base URL and bearer key. It prints the second response for inspection and exits on request or decoding errors. It does not label the email domain ready merely because the lookup succeeded.

Save it as `probe.go` and run it with `go run probe.go`. Set `INFRAI_BASE_URL` to the service's versioned HTTPS base URL, `INFRAI_API_KEY` to the deployment credential, and `DNS_DOMAIN_POINTER` to the JSON Pointer identifying the domain string in the actual DNS response. If the list operation needs query parameters, supply the schema-defined, URL-encoded query through `DNS_LIST_QUERY`. Choose the pointer for the intended domain, not whichever list item happens to appear first.

This configuration is deliberate. A route name does not establish its response fields. The example therefore takes the selector and any query from the published schema instead of inventing a `data.records` envelope. It implements the read-only handoff; the deployment controller's upsert and record comparison remain separate responsibilities.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic("missing environment variable: " + name)
	}
	return v
}

func get(client *http.Client, target, key string) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second * time.Duration(1<<attempt)
			value := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			} else if until, err := http.ParseTime(value); err == nil {
				delay = time.Until(until)
				if delay < 0 {
					delay = 0
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("retry limit reached")
}

func domainAt(body []byte, pointer string) (string, error) {
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return "", err
	}
	if !strings.HasPrefix(pointer, "/") {
		return "", fmt.Errorf("pointer must start with a slash")
	}
	for _, token := range strings.Split(pointer[1:], "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch node := value.(type) {
		case map[string]any:
			value = node[token]
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(node) {
				return "", fmt.Errorf("invalid array index: %q", token)
			}
			value = node[i]
		default:
			return "", fmt.Errorf("pointer cannot traverse %q", token)
		}
	}
	domain, ok := value.(string)
	if !ok || domain == "" || strings.ContainsAny(domain, "/?# \t\r\n") {
		return "", fmt.Errorf("pointer must select a nonempty domain string")
	}
	return domain, nil
}

func run() error {
	base := strings.TrimRight(required("INFRAI_BASE_URL"), "/")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("base URL must be an HTTPS service base without credentials, query, or fragment")
	}
	key := required("INFRAI_API_KEY")
	client := &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	query, err := url.ParseQuery(os.Getenv("DNS_LIST_QUERY"))
	if err != nil {
		return err
	}
	records, err := get(client, base+"/dns/record/list?"+query.Encode(), key)
	if err != nil {
		return err
	}
	domain, err := domainAt(records, required("DNS_DOMAIN_POINTER"))
	if err != nil {
		return err
	}
	mail, err := get(client, base+"/email/domain/get/"+url.PathEscape(domain), key)
	if err != nil {
		return err
	}
	fmt.Println(string(mail))
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

There are 4 total attempts for a rate-limited read, with a 1-second initial exponential delay when no valid `Retry-After` value is present. Those are example policy choices, not service guarantees. Each request has a 15-second timeout. The deployment system should impose its own overall deadline as well, since a server-directed retry delay can exceed the per-request timeout.

Keep this probe out of the write loop. It carries an observed domain identity into the mail lookup, but it does not prove every desired record matches, that SPF or DKIM is correct, or that a warehouse resolver has converged. Those require their own checks. A response body printed to a log is evidence for debugging, not an approval decision.

## Set the page threshold against the release decision

The instrumentation change is to split “DNS deployment succeeded” into configuration agreement and client-path observation. Capture both against the same revision. Block the cutover when owned records differ; retain the old destination while resolver observations remain outside the change plan. An email domain check is an additional gate for shipment notifications, not a replacement for either DNS observation.

A proposed runbook can therefore use 3 distinct states: configuration mismatch, propagation pending, and release eligible. Choose the observation window during rehearsal with the actual resolver path. Page on a persistent condition that needs intervention or a dispatch symptom; keep expected propagation inside the deployment workflow. This avoids turning every intentional record change into an incident.

Thresholds have a cost.

A threshold shorter than the planned observation window can page for an expected stale answer, encouraging the on-call to rerun writes or extend permissions during a healthy transition. A threshold that tolerates every disagreement lets a real drift condition survive until dispatch is affected. The decision should be recorded with the cutover plan: how long to wait, which resolver must agree, who can extend the window, and when to stop. Faster retirement is earned by those observations, not by an earlier successful API response.

## References

These sources describe the alternative products and the standards used by the observation probe. Provider documentation is not a measurement of propagation on the logistics client's resolver path.

- Amazon Route 53 Developer Guide: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html
- Amazon SES domain identity verification: https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html
- Cloudflare DNS documentation: https://developers.cloudflare.com/dns/
- Resend domain documentation: https://resend.com/docs/dashboard/domains/introduction
- RFC 1034, domain name concepts and facilities: https://datatracker.ietf.org/doc/html/rfc1034
- RFC 6901, JSON Pointer: https://datatracker.ietf.org/doc/html/rfc6901

## Further reading

- Go HTTP client behavior: https://pkg.go.dev/net/http#Client
- RFC 9110, Retry-After: https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after
- RFC 7489, DMARC: https://datatracker.ietf.org/doc/html/rfc7489
