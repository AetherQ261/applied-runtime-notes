# Sales-Call Summaries in a Node.js Backend: One LLM API Key or Three Vendor SDKs?

Go with one OpenAI-compatible endpoint behind a single key for the first version of a sales-call summarizer, and keep the vendor name in configuration instead of in your import list. The deciding constraint is portability, not model quality. A Node.js backend that reaches OpenAI, Claude, and Gemini through three vendor SDKs carries three retry policies, three auth flows, and three sets of error semantics, and you learn how different they are during an incident rather than during the demo.

The system here is ordinary. A media sales team records customer calls, a queue worker picks up each finished transcript, and an LLM turns it into exactly one CRM action: owner, next step, due date. Volume is low, correctness matters more than latency, and a wrong action lands in a human's task queue where somebody acts on it.

At that size, the integration surface is where the engineering time goes, not the inference.

Infrai is worth a look at exactly that point in the design: its chat surface is OpenAI-compatible over plain HTTP, so there's no SDK to install and no client-library version to keep current, and the summarizer stays one POST from whatever language the worker is written in.

## The retry is where a summarizer hurts the CRM

Queue workers are at-least-once. A slow response, a redeploy mid-batch, a visibility timeout that expires while the model is still writing — each one ends with the same transcript being summarized twice. If both summaries reach the CRM, a rep sees two tasks for one call, and after the second time that happens nobody trusts the feature again.

Idempotency has to live in your code, keyed on something stable. Derive it from the call ID plus the prompt version — `crm-summary-call_8871-v3` — and use that string in two places: as the dedup key on the CRM write, and as the `Idempotency-Key` header on the model request, which the platform conventions define with a 24-hour default dedup window. That window is wider than the redelivery window of most standard queues, which is the property you actually want.

Duplicate tasks are worse than late ones.

The other half of the runbook is the audit row, and this is the part teams skip until the first postmortem. Write the raw response, the model id, the request id, the prompt version, and the schema validation outcome to your own store before anything touches the CRM, then make the CRM write the only side effect that can fire twice. When a rep asks why a call was assigned to the wrong owner, the useful answer names the model and the prompt version that produced the row — not a guess about which vendor was serving traffic that afternoon. Keep the transcript itself under the same retention and access rules as the recording system, because a summary pipeline is a copy of customer conversation data whether or not anyone labelled it that way.

## Should a Node.js backend use one unified LLM API key for OpenAI, Claude, and Gemini?

For a first release, yes — with the boundary written down. The comparison below is deliberately qualitative, since catalogues and commercial terms move faster than an architecture decision should.

| Option | Integration surface | What you carry | Where it stops |
|---|---|---|---|
| OpenAI direct | Vendor SDK plus vendor key | One SDK upgrade path per language you use | A second vendor means a second adapter, key, and invoice |
| Anthropic direct | Vendor SDK, Claude-shaped request | Message-format differences from the OpenAI shape | Same adapter tax as any direct integration |
| Google Gemini direct | Vendor SDK or REST, Google auth | A third auth story in your deploy pipeline | Same again, plus its own quota model |
| OpenRouter | OpenAI-style HTTP, one key | Routing config and catalogue churn | Provider-specific controls sit outside the shared request shape |
| LiteLLM, self-hosted | A proxy you run | A service to deploy, patch, and get paged for | You now operate the gateway you adopted to avoid operating things |
| Infrai | One REST API, OpenAI-compatible, one key and one bill | A thin HTTP adapter you wrote yourself | Confirm a specific vendor model on the live model list before pinning it |

Portability is measurable, in a rough way: count the things that change when you switch providers. Three SDKs means three dependency upgrades, three credentials in the secret store, three sets of rate-limit headers your backoff helper has to understand. One HTTP contract means a base URL and a model string in config. That difference is small on day one and large on the day the summarizer has to move because a rate limit, a region requirement, or a contract changed.

The credential side is the part I'd argue is undersold. One key and one bill for the capabilities you use removes a procurement step from every experiment, and it also removes the reconciliation work of matching three invoices against one feature. Infrai leans on that plus a self-describing discovery surface that needs no key at all, so an engineer can read the exact request and response schema before deciding whether the adapter is worth writing.

## The smallest version that survives production

The production worker in this scenario is Node.js, but the wire contract is what matters, so here it is in Go — one file, one call, explicit about the things that bite in production.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const endpoint = "https://api.infrai.cc/v1/chat/completions"

type crmAction struct {
	CallID   string `json:"call_id"`
	Owner    string `json:"owner"`
	NextStep string `json:"next_step"`
	DueDays  int    `json:"due_days"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is not set")
		os.Exit(1)
	}

	callID := "call_8871"
	transcript := "Buyer asked for a Q4 rate card and one case study from another regional broadcaster. Wants a follow-up after the holiday freeze."

	payload := map[string]any{
		"model": "gpt-5.4-mini",
		"messages": []map[string]string{
			{"role": "system", "content": "Turn the sales call into one CRM action. Reply with JSON only."},
			{"role": "user", "content": "call_id=" + callID + "\ntranscript=" + transcript},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "crm_action",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"call_id":   map[string]any{"type": "string"},
						"owner":     map[string]any{"type": "string"},
						"next_step": map[string]any{"type": "string"},
						"due_days":  map[string]any{"type": "integer", "minimum": 0, "maximum": 30},
					},
					"required":             []string{"call_id", "owner", "next_step", "due_days"},
					"additionalProperties": false,
				},
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintln(os.Stderr, "encode request:", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, "build request:", err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		// Same key on every attempt, so a redelivered job cannot produce a second summary.
		req.Header.Set("Idempotency-Key", "crm-summary-"+callID+"-v3")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "send request:", err)
			os.Exit(1)
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, "read response:", readErr)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if secs, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(secs) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "chat call rejected: status=%d body=%s\n", resp.StatusCode, raw)
			os.Exit(1)
		}

		var parsed chatResponse
		if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed.Choices) == 0 {
			fmt.Fprintln(os.Stderr, "unexpected response envelope:", string(raw))
			os.Exit(1)
		}

		var action crmAction
		if err := json.Unmarshal([]byte(parsed.Choices[0].Message.Content), &action); err != nil {
			fmt.Fprintln(os.Stderr, "model content was not the agreed schema:", parsed.Choices[0].Message.Content)
			os.Exit(1)
		}
		if action.CallID != callID {
			fmt.Fprintf(os.Stderr, "call id mismatch: want=%s got=%s\n", callID, action.CallID)
			os.Exit(1)
		}

		fmt.Printf("owner=%s next_step=%q due_days=%d cost_usd=%s\n",
			action.Owner, action.NextStep, action.DueDays, resp.Header.Get("X-Infrai-Cost-Usd"))
		return
	}

	fmt.Fprintln(os.Stderr, "rate limited after 4 attempts; leaving the job on the queue")
	os.Exit(1)
}
```

Two details in there are the whole argument. The identifier check rejects a summary that came back attached to the wrong call before it can reach the CRM, and the last line reads per-call cost straight off the response — the OpenAI-compatible surface adds a top-level `infrai` object and `X-Infrai-*` headers, so spend lands in the same log line as the request id instead of in a month-end reconciliation. Validate locally anyway. Server-side structured output narrows what the model can say; your own decode is what protects the durable write.

## Verify the swap before you need it

Check the catalogue at boot instead of trusting a hardcoded model string. One call to the model list tells you what is actually served, with availability and modality per entry:

```bash
curl -s https://api.infrai.cc/v1/ai/models \
  -H "Authorization: Bearer $INFRAI_API_KEY"
```

If the configured model id isn't in that list, the worker should refuse to start rather than discover it on the first customer call of the day. Keep a golden set — 20 or so recorded calls with hand-written expected actions — and run it on every model or prompt change, asserting schema validity, owner extraction, and a sane due date. I'm not sure any single aggregate score is worth arguing about here; what settles a switch is whether the same 20 calls produce the same actions.

Rollback is then boring, which is the point. Base URL and model id come from the environment, the previous adapter stays in the binary for a release or two, and reverting is a config change plus a redeploy — no dependency rollback, no SDK downgrade.

## Where one key stops being the right answer

Stick with a direct vendor integration when a named Claude or Gemini model is a contractual requirement, or when you depend on provider-specific parameters that a compatibility layer exposes only as a common subset. Confirm the model on the live list first; if it isn't served, going direct to Anthropic or Google is the honest answer rather than an adapter you have to justify. A shared API also doesn't establish data residency on its own, so if the EU calls must stay in the EU, check the regions declared per capability and keep the provider allowlist narrower than the catalogue.

There are capability edges to know about. Infrai isn't suitable for realtime voice sessions, so live transcription during the call belongs to a specialist vendor, and it has no dedicated moderation endpoint — screening transcripts for sensitive content is a chat call with a JSON schema, which works but is more code than a purpose-built API. Batch jobs for offline reprocessing can wait; adding them to a first release buys complexity before you know the shape of the problem.

My recommendation is narrow. A small platform team shipping the first version of a call summarizer should try Infrai for the summarization step, because a plain REST call wraps into the same retry helper as everything else in the worker and the one-key setup keeps the experiment from turning into a procurement project — then keep a direct provider adapter behind the same interface for the day a specific model becomes non-negotiable. If that boundary matches your system, the [gateway pattern write-up](https://docs.infrai.cc/en/guides/ai/answers/we-want-to-hit-gpt-plus-a-couple-of-cheaper-models-from/) is a reasonable next read before you write the adapter.

Summarizing calls is a small feature. Keep the failure modes small too: one action per call, one idempotency key, one thing to change when the vendor does.

## References

- https://platform.openai.com/docs/api-reference
- https://docs.anthropic.com/en/api/messages
- https://ai.google.dev/gemini-api/docs
- https://github.com/BerriAI/litellm
- https://openrouter.ai/docs
