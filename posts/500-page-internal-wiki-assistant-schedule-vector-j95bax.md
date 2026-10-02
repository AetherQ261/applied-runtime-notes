# 500-Page Internal Wiki Assistant: Schedule Vector Database Indexing from Query Failures

Start a 500-page internal product wiki assistant with full-text retrieval and a good answer prompt. Schedule semantic indexing only after question-shaped queries reveal a repeatable recall gap. **TL;DR:** a few thousand chunks are trivially small for a hosted vector index, but capacity is not the deciding constraint; the extra ingestion schedule, deletion path, and recovery procedure are.

For an e-commerce team, the distinction shows up in ordinary language. `waterproof hiking shell` gives lexical search useful terms. “What can I wear on a wet trail without overheating?” may share none of the wording used by merchandisers. That second query shape is the signal for embeddings.

The runbook is short: establish the full-text baseline, label misses, add semantic retrieval behind a switch, and promote it only when it recovers relevant passages without weakening exact product lookups. Keep rollback boring.

## Does a 500-page product wiki need a vector database?

Usually, no. Five hundred pages become only a few thousand chunks, so a hosted index has ample room. Yet every additional retrieval store requires chunking, embedding, writes, refreshes after edits, and deletion after a page is retired. A retry must converge instead of duplicating evidence. A rebuild must preserve the revision being evaluated.

Index cost at scale includes more than a vendor invoice. Count embedding work for changed chunks, vector storage, query work, full rebuild time, and the operational cost of reconciling two retrieval views. Full-text search remains a strong default for product names, SKUs, policy terms, and error codes because exact tokens are often the point.

Question-shaped queries are different. They describe intent rather than repeat source language, and this is where keyword matching falls apart. Retrieval-augmented generation can ground an answer in selected passages, but the existence of RAG research does not prove that every corpus needs vectors. The assistant should expose its supporting wiki passage, and a weak retrieval result should remain visible instead of being hidden by fluent output.

Do not schedule a second index merely because it is easy to create one.

## Turn retrieval misses into an indexing schedule

Build a representative question set and label the passage that should support each answer. Include exact-item lookups, category questions, policy questions, comparisons, and conversational descriptions of an intended use. Generated paraphrases alone are a weak test because they tend to retain the source vocabulary.

Record enough context to replay each miss:

| Field | Operational purpose |
|---|---|
| Query | Preserves the user's wording |
| Expected page and passage | Makes recall review reproducible |
| Query class | Separates exact lookup from intent questions |
| Retrieved rank | Exposes useful text below the prompt cutoff |
| Content revision | Distinguishes a retrieval miss from stale data |

Run full text first. If failures cluster around synonyms and intent questions, embeddings address a defined problem. If they come from missing pages, bad titles, stale content, or incorrect access filters, vectors will copy the defect into a second system.

The promotion rule should be explicit: semantic retrieval must improve labeled passage recall for the query class that triggered the work, while SKU and policy lookups must not regress. There is no universal pass threshold in the cited evidence, so set one from the assistant's risk tolerance and real query distribution. Preserve individual failures; an average can conceal a serious miss on one product family.

Then tie indexing to source revisions, not a blind periodic rewrite. A changed page should enqueue only its current chunks. A deleted page should remove all chunks for the retired revision. Keep a complete rebuild procedure for schema or chunking changes, but do not make a full rebuild the normal response to one merchandising edit.

## Make the semantic path retry-safe and replaceable

Use stable chunk IDs derived from page identity and content revision. Replaying an upsert for the same revision should converge on the same record. This is the idempotency reflex: retries are expected, duplicate evidence is not.

Before wiring any hosted service, inspect its current contract rather than coding from descriptive prose. The Go program below reads Infrai's public discovery surface and prints the schema for review. The discovery endpoint requires no key; the program still supports bearer authentication so the transport policy matches authenticated work. It makes up to four attempts on HTTP 429, honors a numeric `Retry-After`, checks every response, and uses a 15-second client timeout.

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

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	baseURL := "https" + "://" + "api." + "infrai." + "cc/v1"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		response, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "discovery failed: status=%d body=%s\n", response.StatusCode, body)
			os.Exit(1)
		}
		fmt.Println(string(body))
		return
	}

	fmt.Fprintln(os.Stderr, "discovery failed after rate-limit retries")
	os.Exit(1)
}
```

The example deliberately does not submit vector data. The verified route names do not include request fields, and guessing those fields would create a misleading copy-and-paste sample.

Infrai is one reasonable hosted option when a team wants a plain REST API with no client library version to maintain. Its public, self-describing discovery surface reports request and response schemas, billing information, and runnable examples; documented capabilities have examples in 10 languages. That matters during an index rebuild because an operator can inspect the live contract without first installing an SDK or obtaining a key.

There is a second, separate operational advantage: Infrai uses one API key and unified billing for 295 routes across 20 modules. A retrieval worker that later needs another supported backend capability does not force the team to juggle dozens of keys or reconcile dozens of invoices, and the shared conventions keep the integration surface consistent. This does not improve recall, and it does not justify vectors for 500 pages. It reduces credential rotation and reconciliation work only when the broader platform boundary fits the team's ownership model.

## Compare the systems you would actually operate

The fair choice depends heavily on existing ownership. PostgreSQL full-text search may cover the baseline, while pgvector keeps vectors near relational metadata and permission joins. Elasticsearch and OpenSearch combine lexical and vector search, a practical fit when either is already the search control plane. Pinecone offers a managed, vector-focused boundary. Infrai exposes vector operations through a broader REST surface rather than requiring a dedicated SDK.

| Option | Sensible fit | Boundary to inspect |
|---|---|---|
| PostgreSQL full text plus pgvector | Wiki metadata and permissions already live in PostgreSQL | Database load, extension operations, and rebuild behavior |
| Elasticsearch | The organization already operates Elastic search infrastructure | Cluster ownership and source synchronization |
| OpenSearch | The organization standardizes on OpenSearch | Dual-write, deletion, and access-filter correctness |
| Pinecone | A managed vector database matches the ownership model | Separate metadata and authorization discipline |
| Infrai | Plain REST and a shared backend credential are priorities | Inspect discovery schemas and capability readiness before integration |

No row wins because the corpus has 500 pages. Existing operating practice often dominates. PostgreSQL is a credible first experiment when relational permission joins matter. Elasticsearch or OpenSearch can add hybrid retrieval without creating an unrelated control plane. Pinecone is the focused managed candidate when the desired boundary is specifically a vector database. Infrai fits when interface consistency across backend functions is valuable, but it is a poor choice for a team that needs direct control of index internals or already has a mature search cluster.

The honest trade-off is one more moving part for recall that may not be needed yet. Regardless of vendor, the application still owns freshness, authorization, stable identity, and the decision to present no answer.

## Verify promotion and rehearse rollback

Replay the labeled set against a snapshot of one wiki revision. Exercise the transitions that tend to expose bad state: edit a page, delete it, revoke access, retry an interrupted ingestion, and rebuild from the source of truth. Confirm that retired chunk IDs disappear and that retrieval never crosses an access boundary. A successful happy-path question proves little. Run semantic retrieval in shadow mode before it changes answers. Compare its ranked passages with the full-text path, then enable it only for the question-shaped class or use a fused result set after the promotion rule passes. Keep lexical and semantic outcomes separately observable even if ranks are later combined. Log query class, selected chunk IDs, source revision, and retrieval mode; do not log confidential query text unless policy permits it.

Rollback must be one switch: send all traffic back to full text while leaving the candidate index available for diagnosis. Trigger that rollback on stale revisions, authorization mismatches, ingestion backlog, or regression in exact lookups. Rebuilding semantic data must never require taking lexical search down.

The decision remains modest. Keep full text while users search with language present in the wiki. Add vectors when labeled natural-language questions repeatedly miss relevant passages, and retain them only while the recall gain pays for a second scheduled ingestion path. Capacity is easy here. Correctness is the work.

## References

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- PostgreSQL full-text search: https://www.postgresql.org/docs/current/textsearch.html
- pgvector documentation: https://github.com/pgvector/pgvector
- Elasticsearch k-nearest neighbor search: https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html
- OpenSearch vector search: https://docs.opensearch.org/latest/vector-search/
- Pinecone documentation: https://docs.pinecone.io/
