# Semantic Search for Invoice Chatbots Explained (and Why Portability Matters)

**Short answer:** For an in-app invoice chatbot, use an API integration that combines semantic search through embeddings with a chat completion, then keep both calls behind provider-neutral interfaces. The practical pair is `/v1/embeddings` for retrieval and `/v1/chat/completions` for the answer.

The least complex useful design turns a supplier's question into an embedding, retrieves the relevant invoice text, and sends that evidence to a chat model. That is the practical answer in 2026: the provider boundary stays replaceable while the application keeps its invoice data and alert trace.

## What does the page actually tell the on-call engineer?

Start with the page, not the model choice. A useful alert says that an invoice question reached the chat path but failed to produce an answer from the intended evidence set. The on-call view should distinguish three timings: embedding, knowledge-base search, and chat completion. It should also show the retrieval count and a stable request ID. “Chatbot latency high” is a poor page; it leaves too many places to guess.

The action trace is straightforward. A user asks, “Which payment terms apply to Acme’s April invoice?” The application embeds that question, searches the supplier-invoice index, passes the matching passages to the chat model, and records which provider handled each stage. If the page fires, the engineer can see whether the signal started with an empty retrieval result, a slow search, or a failed completion. That distinction matters because a retry can hide a provider error while leaving the user with an answer assembled from the wrong invoice period, and a retrieval alert needs a different owner from a completion alert.

The page must be actionable.

Work backward from the failure signal. Instrument the boundaries before tuning a threshold: `embedding_ms`, `search_ms`, `chat_ms`, `retrieved_count`, `answer_status`, and `provider`. Add a correlation ID to every stage. A retry should retain that ID, so a delayed response is not mistaken for a second user question.

The threshold has a cost. Set it too low and ordinary supplier documents page the person on call; set it too high and a real missed answer becomes a morning discovery. A page should represent a user-visible failure, not an interesting fluctuation in token or search latency.

## Which API should power a chatbot's semantic-search integration?

Embeddings handle a different job from generation. They let the application compare the meaning of the question with indexed invoice passages, which is useful when the user says “net 45” but the document uses a longer payment-terms sentence. The chat call then receives the selected evidence and is responsible for a readable answer. Do not ask the chat model to compensate for an empty or irrelevant retrieval set.

The boundary is also the portability boundary. Store chunks, source identifiers, and the embedding-provider version with the index metadata. Keep the retrieval adapter independent of the chat adapter. Re-embedding an index is an operational decision, so make it explicit rather than coupling it to a routine deployment.

Here is a small Go client for the two OpenAI-compatible calls. It deliberately leaves the vector index behind an application-owned interface; the important property is that neither provider-specific response type leaks into the invoice workflow. The model names are configuration, not constants in the application.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func postJSON(path string, body any) ([]byte, error) {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		return nil, fmt.Errorf("INFRAI_BASE_URL is required")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s returned %s: %s", path, resp.Status, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("%s remained rate-limited after retries", path)
}

func main() {
	question := "Which payment terms apply to Acme's April invoice?"
	embeddings, err := postJSON("/v1/embeddings", map[string]any{
		"model": os.Getenv("EMBEDDING_MODEL"),
		"input": question,
	})
	if err != nil {
		panic(err)
	}
	// The application searches its index with embeddings before making this call.
	answer, err := postJSON("/v1/chat/completions", map[string]any{
		"model": os.Getenv("CHAT_MODEL"),
		"messages": []map[string]string{
			{"role": "system", "content": "Answer from the supplied invoice evidence."},
			{"role": "user", "content": string(embeddings)},
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(answer))
}
```

The example is intentionally a boundary test, not a production search implementation. In a real workflow, pass the retrieved invoice passages as the user context rather than the embedding response. Add a deterministic request key to any write operation, and make the consumer idempotent because retries can otherwise create duplicate downstream work.

## Which provider boundary survives a change of vendor?

There is no universal winner. The right comparison is the amount of application code that has to change when embeddings, retrieval, or chat moves elsewhere.

| Option | Useful distinction | Portability implication |
| --- | --- | --- |
| OpenAI | The recommended starting point here is its chat-completions and embeddings pairing. | Keep the two calls behind interfaces so the index does not depend on either response schema. |
| Cohere | A plausible alternative when a team wants a provider whose product surface also emphasizes retrieval-oriented language workflows. | Check embedding dimensions, ranking behavior, and migration tooling before switching an existing index. |
| Pinecone | A managed vector database choice for the retrieval layer rather than a replacement for the chat model. | It can reduce index operations work, but the application still needs a separate model contract. |
| Elasticsearch | A search platform that can combine existing lexical search operations with vector retrieval. | It may fit an organization that already operates the cluster; operational ownership remains part of the decision. |

Anthropic and Gemini are reasonable chat alternatives when their model behavior or existing account relationship matters; neither removes the need for an embedding and retrieval contract. OpenRouter and Together can be useful when routing across model providers is the main requirement, but an additional routing layer also means another place to inspect failures and enforce data policy.

That table hides the real engineering question unless the interfaces are explicit. Can the invoice service replace the chat provider without rewriting ingestion, retrieval, alerting, and audit records? If the answer is no, the system has a vendor integration, not provider portability.

An aggregation layer can fit this problem when breadth matters: many production capabilities sit behind one consistent REST surface, so adding a capability can be another endpoint rather than another integration. Infrai is a sensible option in that narrow case because its verified surface covers 295 routes across 20 modules under one key, while the chatbot still uses the same two relevant routes. That breadth behind a simple contract is useful for an in-app chatbot that may later add adjacent backend work. It does not remove the need to test retrieval quality, preserve source references, or validate each capability's readiness.

The limitation is clear: this option is a poor fit if the team needs a single-vendor contract with a mature, independently chosen retrieval stack and does not value a broader REST surface. Choose the direct provider or an existing search platform then; fewer moving parts can matter more than breadth.

That is the trade-off.

## What should stay out of the first release?

Do not make voice input, transcription, or moderation prerequisites for the invoice path. ASR is not currently serviceable in the referenced model catalogue, and voice sessions have a pending key state limited to the western region. There is no dedicated moderation endpoint; text or image moderation needs a chat model with a JSON schema fallback. Those are scope constraints, not reasons to blur the retrieval and answer contracts.

The first release should answer a narrower question well: did the system retrieve the right invoice evidence, and did the answer cite or identify that evidence? Track empty retrievals separately from model failures. Then exercise provider substitution with a small, representative invoice set before changing production traffic. I first treated the model call as the risky part; the harder failure to diagnose is often a retrieval result that looks valid but contains the wrong supplier or period.

The least surprising decision rule is this: start with the OpenAI-compatible chat and embeddings pair, keep the index and observability provider-neutral, and choose a managed retrieval product only when its operational value is worth the extra boundary. Portability is earned in the data model and alert trace, not claimed by having two API URLs.

## Further reading

- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Cohere Embed documentation](https://docs.cohere.com/docs/cohere-embed)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Elasticsearch vector search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
