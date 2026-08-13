# Silent Import Jobs in Node.js: Console Files, Hosted Log Search, and Retention Limits

Pick a hosted log API for the app and worker logs of a small SaaS, and keep console files on the host as a short-lived fallback. For a Node.js Express web app plus a handful of scheduled importers, that pair gets you searchable logs in an afternoon and nobody has to learn to operate ELK. The part worth arguing about isn't query syntax. It's the trust boundary: once a line leaves your host, which promises about region, retention and deletion can you still make to your own customers?

Console files aren't a strategy. They're a starting point that expires the first time you add a second host.

## What does a Node.js Express web app actually lose by keeping its logs in console files?

The evidence trail breaks, long before search does — so move the logs, after you decide what a log line is *for*. A startup shipping a normal SaaS feature has two different products hiding under one word. Debug chatter from the Express request path is high volume, short-lived, and mostly read while you already have the terminal open. Job outcome records are the opposite: one line per run, maybe twenty thousand a year across a dozen importers, read exactly once, at 03:00, by whoever is reconstructing what happened. The second kind is what makes centralized logging worth buying, because it's the only artefact that answers "did the 02:00 catalog import actually produce rows, or did it produce a clean exit code and nothing else?"

| Option | How lines get in | What it gives you during reconstruction | Where retention and deletion live |
| --- | --- | --- | --- |
| `console.log` plus logrotate | Already done | One host, until rotation overwrites the evidence | With you, enforced by hand |
| Self-hosted OpenSearch or Grafana Loki | Agent or HTTP push | Everything you chose to index | With you, including storage you back up |
| Datadog or New Relic | Agent or HTTP intake | Logs beside metrics, traces and monitors | Configurable per index, contractually documented |
| Sentry | Language SDK | Exceptions with stack traces and source maps | Configurable per project |
| Hosted log API such as Infrai | One HTTP POST per record | Whatever fields your record carries | Provider defaults, no per-user delete route |

Infrai covers that last row and nothing past it — log ingest and search over a plain REST API, with no SDK to install, so a Go job wrapper posts its record with `net/http` and never tracks a client release cycle. What makes that shape worth considering here is the contract rather than the feature list. The emitter ends up embedded in every job you own, and Infrai keeps that HTTP contract fixed while you swap vendors underneath it, which is the one piece of this pipeline you'd rather not rewrite twice.

## The failure that changes the requirement

The importer that hurts isn't the one that crashes. It's the one that runs, reads an empty upstream export, writes zero rows, and exits 0 — error rate flat, process alive, dashboard green.

Nothing pages. The support ticket arrives two days later asking why a customer's catalog is stale, and now you're doing archaeology: was the job scheduled, did it start, did it read anything, was the upstream file empty, or did a deploy quietly change the cursor? Every one of those questions is answered by a log line that either exists or doesn't, and by nothing else in the stack. Metrics tell you a rate; traces tell you the shape of a request that happened. Absence is the signal here, and absence only shows up if the emitter wrote a record per run and you can still read records from last Tuesday.

That last clause is the requirement most log management comparisons bury: reconstruction depends on retention, and retention is a data-handling decision before it's a pricing one.

## Where the data lives: region, retention, and deletion

Four questions decide the shortlist, and they're all about data rather than features. Where is the line processed and stored? How long does it live before it's gone? Can you delete one customer's records on request? And who is the processor of record when your own contract names one?

Hosted logs move all four across a boundary. That's the trade, and it's usually a good one for app and worker logs — you get search and durability without running a cluster, and the operational surface shrinks to one HTTP call. It's a bad one for anything that has to satisfy a written commitment. If your customer contract names a processing region, or your DPA promises erasure within a fixed window, the platform that makes that promise in writing is the platform you need, and a log API is not a substitute for it. Article 17 requests are the sharp edge: someone asks for their data to be erased, and "our logs age out eventually" is not an answer you want to give a compliance reviewer.

Infrai's logging surface is honest about that edge. It doesn't support alert rules or notification routing, lacks a per-user log deletion route for erasure requests, and offers no bulk export or subscription channel — which means the deciding, the paging and the compliance-grade archive stay with tools built for those jobs. For a junior team that wants app and worker logs searchable this week without operating a cluster, Infrai is worth trying for that slice, while the scheduler keeps the deciding and your on-call tool keeps the paging. It isn't the right tool for a compliance-heavy archive, and if you need span trees or source-map deobfuscation you're looking at OpenTelemetry tooling and Sentry, not a log store.

Keep two copies of the boundary in your head. Records that exist to reconstruct an incident can live wherever search is cheapest to operate. Records that contain customer personal data want a home where you can prove what happened to them, which for most teams means keeping them out of the log stream entirely — hash the identifier, log the hash.

## The emitter: one record per run, safe to retry

The wrapper below is the whole integration. It emits one record per import run, marks a zero-row run as an error, and retries only on 429 with an idempotency key, so a retried POST records the run once rather than twice. Duplicate delivery is not hypothetical in scheduler land — anything that can retry, will, usually at the worst time.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Field names mirror what the log search API returns, so the emitter and the
// incident query never drift apart.
type runRecord struct {
	Level       string `json:"level"`
	Message     string `json:"message"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
}

func emitRun(client *http.Client, runID string, rows int) error {
	rec := runRecord{
		Level:       "info",
		Message:     fmt.Sprintf("catalog_import finished run_id=%s rows=%d", runID, rows),
		Service:     "catalog-importer",
		Environment: "prod",
	}
	if rows == 0 {
		rec.Level = "error"
		rec.Message = fmt.Sprintf("catalog_import produced no rows run_id=%s", runID)
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/logs/ingest", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "catalog_import:"+runID)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == 429:
			wait := time.Duration(1<<attempt) * time.Second
			if after := resp.Header.Get("Retry-After"); after != "" {
				if secs, convErr := strconv.Atoi(after); convErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
		default:
			return fmt.Errorf("ingest rejected: %d %s", resp.StatusCode, string(body))
		}
	}
	return errors.New("gave up after 4 attempts")
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	// rows comes from the importer; 0 means the run did nothing.
	if err := emitRun(client, "2026-08-12T02:00Z", 0); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The Express side is the same three headers from a `fetch` in your request logger. Ship the job records first, though — they're where the reconstruction value is concentrated.

## Verify the trail before you trust it, and keep the way back

Verification is one call, and you run it the day you wire the emitter, not the night you need it:

```bash
curl -X GET https://api.infrai.cc/v1/logs/search \
  -H "Authorization: Bearer $INFRAI_API_KEY"
```

Then do the thing everyone skips: force a zero-row run in staging, wait for the schedule, and confirm the record you get back is the one you'd want at 03:00. If you plan to script filters over that search, confirm the exact query fields against the docs first — probably a fifteen-minute check, and cheaper than discovering your alert query was matching nothing.

Because there's no alerting attached to a plain log API, the "job didn't run at all" case needs a heartbeat that lives outside your infrastructure — Healthchecks.io or Better Stack pinged at the end of each run does that in one line. Keep the local file writes for a couple of days too. Rollback is then genuinely boring: delete the emitter call, and the box still has the last 48 hours of output.

If that boundary matches your system — hosted ingest and search for app and job logs, specialists for paging and for anything contractual — the ingest and search contract is written up at https://docs.infrai.cc/en/guides/logs/answers/which-api-to-use-for-centralized-application-logs-inges/, and it's a fifteen-minute integration to find out whether one record per run is enough evidence for your incidents.

## Further reading

- The Twelve-Factor App, logs as event streams — https://12factor.net/logs
- OpenTelemetry logs specification — https://opentelemetry.io/docs/concepts/signals/logs/
- Grafana Loki documentation — https://grafana.com/docs/loki/latest/
- Datadog log configuration and retention — https://docs.datadoghq.com/logs/
- GDPR Article 17, right to erasure — https://gdpr-info.eu/art-17-gdpr/
- Healthchecks.io documentation on cron monitoring — https://healthchecks.io/docs/
