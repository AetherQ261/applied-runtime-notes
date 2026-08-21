# Tenant-Charged Catalog Calls: Node.js LLM Text Classification Returning Exact JSON Labels

Treat multi-label product tagging as a closed-set queue operation: make the LLM return one JSON object, reject every label outside the tenant's versioned taxonomy, and write CRM actions only through an idempotent commit. For an ecommerce team summarizing sales calls, that boundary matters more than prompt wording because retries, taxonomy changes, and mixed tenant traffic are where a plausible demo becomes an unreliable production job.

Short answer: a Node.js worker should ask for exact labels from an allowed list, parse the response as strict JSON, validate membership and cardinality, then persist the result with the tenant ID, taxonomy version, request ID, and usage record in one observable workflow.

Don't let generated strings become database keys.

## Cost attribution begins at queue admission

The useful mental model isn't "send text, get tags." It is `received -> classified -> validated -> committed`, with a terminal quarantine state for output that cannot pass the contract. A sales-call summarizer may extract a mention such as "the buyer wants a waterproof trail shoe for winter inventory" and propose product labels, but only the validator is allowed to turn those labels into CRM fields or follow-up actions.

This split contains two operational failures. First, a retry can classify the same call twice. Second, a model can return a near-match that looks fine to a person but creates a new taxonomy value, such as `trail-shoes` when the registered label is `trail_footwear`. The commit key should therefore be derived from stable inputs such as tenant ID, call ID, job kind, and taxonomy version. A redelivery may repeat classification, but it must converge on the same CRM write. The accounting record needs a different grain: one row per attempt, including attempts that never produce a business result, connected to one logical job and at most one committed CRM action. Otherwise retry overhead disappears from tenant reports or, worse, each retry looks like another completed sales-call summary.

I've been paged by missed jobs and duplicate deliveries. The runbook reflex from those incidents is simple: acknowledge the queue message only after the durable commit, and make that commit safe to repeat. If the process stops after validation but before acknowledgement, the next delivery should find the existing commit rather than create another action.

Per-tenant cost visibility belongs in this state machine, too. Attach metering to the request ID and tenant ID before dispatch, then finalize the record beside the classification outcome. A shared monthly total can't explain which tenant, taxonomy version, or replay policy produced the load. Store provider-reported input and output units when they are available; if they aren't available, mark the measurement unknown rather than estimating it from a different tokenizer. I'm not sure a given provider's usage fields will remain comparable across models, so the raw provider fields and model identifier should be retained instead of collapsed into one supposedly universal number.

Retries cost twice.

## How should Node.js LLM classification return exact ecommerce labels?

Use a narrow wire contract. The request carries immutable identifiers, the source text, the exact allowed label strings, and explicit minimum and maximum counts. The response carries only the labels and the same taxonomy version. JSON syntax alone is insufficient: `{"labels":["trail-shoes"]}` is valid JSON and still invalid for this job.

The following contract is what the Node.js queue worker should enforce at its boundary, regardless of the model client behind it:

| Field | Rule | Failure action |
|---|---|---|
| `tenant_id` | Required and equal to the queued tenant | Stop; do not classify |
| `taxonomy_version` | Exact match with the job snapshot | Requeue under the current version or quarantine |
| `labels` | Array of unique strings from the allowlist | Quarantine the output |
| Label count | Within the job's declared bounds | Quarantine the output |
| Request ID | Stable across delivery retries | Reuse the prior committed result |

Keep the prompt and the validator separate. The prompt can state that labels must be copied byte-for-byte from the allowlist and that no explanation is wanted. The validator must assume those instructions may be ignored. Case folding, trimming, or fuzzy matching at validation time is tempting, but each one silently changes the taxonomy contract. If aliases are required, define an explicit, versioned alias map before inference and resolve aliases to canonical labels before they reach the allowlist.

Order is another choice to make deliberately. If downstream CRM behavior treats the label array as a set, sort the validated labels before hashing or storage. That prevents harmless output order changes from producing different payload hashes. If rank carries meaning, encode rank as a separate field and validate it; don't smuggle priority into array position.

## Integration boundary: validate before ledger finalization

All provider-specific calls should sit behind a small adapter that returns response bytes plus raw usage metadata. The core classifier then has no SDK types in its contract. A Node.js worker can call that boundary over plain HTTP or execute an equivalent local module; the verifier below is written in Go because the important artifact is the wire behavior, not a particular client library.

```go
package classification

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

type Job struct {
	TenantID       string
	CallID         string
	RequestID      string
	TaxonomyVersion string
	AllowedLabels  []string
	MinLabels      int
	MaxLabels      int
}

type ModelOutput struct {
	Labels          []string `json:"labels"`
	TaxonomyVersion string   `json:"taxonomy_version"`
}

type ValidatedResult struct {
	TenantID        string
	CallID          string
	RequestID       string
	TaxonomyVersion string
	Labels          []string
}

func Validate(raw []byte, job Job) (ValidatedResult, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var out ModelOutput
	if err := dec.Decode(&out); err != nil {
		return ValidatedResult{}, fmt.Errorf("decode model output: %w", err)
	}
	if err := ensureEOF(dec); err != nil {
		return ValidatedResult{}, err
	}
	if out.TaxonomyVersion != job.TaxonomyVersion {
		return ValidatedResult{}, errors.New("taxonomy version mismatch")
	}
	if len(out.Labels) < job.MinLabels || len(out.Labels) > job.MaxLabels {
		return ValidatedResult{}, errors.New("label count outside configured bounds")
	}

	allowed := make(map[string]struct{}, len(job.AllowedLabels))
	for _, label := range job.AllowedLabels {
		allowed[label] = struct{}{}
	}

	seen := make(map[string]struct{}, len(out.Labels))
	for _, label := range out.Labels {
		if _, ok := allowed[label]; !ok {
			return ValidatedResult{}, fmt.Errorf("label %q is not in the taxonomy", label)
		}
		if _, duplicate := seen[label]; duplicate {
			return ValidatedResult{}, fmt.Errorf("duplicate label %q", label)
		}
		seen[label] = struct{}{}
	}

	sort.Strings(out.Labels)
	return ValidatedResult{
		TenantID:        job.TenantID,
		CallID:          job.CallID,
		RequestID:       job.RequestID,
		TaxonomyVersion: job.TaxonomyVersion,
		Labels:          out.Labels,
	}, nil
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("read trailing data: %w", err)
	}
	return errors.New("model output contains trailing JSON")
}
```

For a taxonomy version named `catalog-2026-04`, an accepted model response could contain `{"labels":["winter","trail_footwear"],"taxonomy_version":"catalog-2026-04"}`. The same response with an explanation field is rejected, as is a second JSON object after the first. Fail closed. The rejected response should be retained under the organization's normal data-handling policy with its request ID and validation reason, but it must not be copied into CRM fields.

The adapter should distinguish retryable transport outcomes from contract rejection. A timeout before a response may be retried with the same request ID and idempotency key. Invalid JSON, an unknown label, or a version mismatch is not improved by a blind immediate retry; send it to a bounded review or quarantine path. Avoid infinite loops disguised as resilience. In the ledger, record those paths differently: `transport_retry` means another attempt may follow, while `contract_rejected` means the attempt finished but no CRM mutation was authorized. That distinction makes a tenant's higher consumption explainable without pretending every consumed unit created value.

There is also a scheduling race worth naming. Taxonomy version `v18` may be current when a call enters the queue and retired when a delayed worker receives it. Either process against the snapshotted `v18` contract and record that version, or deliberately migrate the job to `v19` and give the migration a new request identity. Loading "latest" halfway through a retry makes the same job nondeterministic.

## Test retry accounting separately from label quality

Test the validator with generated combinations, not just three hand-written happy paths. At minimum, cover every allowed label individually, multiple valid labels in different orders, duplicate labels, empty arrays, counts over the maximum, unknown strings, wrong case, unknown JSON fields, trailing JSON, and taxonomy-version mismatch. The model evaluation set should separately include short titles, noisy sales-call transcripts, negation, several products in one call, and text that supports no allowed label. Validation tests prove containment; evaluation tests measure whether the contained answer is useful. They are different gates.

One job. One commit.

Use two alerts with different owners. Queue age or missed schedule windows indicate delivery trouble and belong to the runtime on-call. A jump in unknown-label or malformed-output rejection indicates a contract or model-quality issue and belongs to the classifier owner. Combining both into "tagging failed" makes triage slower — especially at 03:00, when the first question is whether work was never attempted or was attempted and safely refused.

Cost attribution follows the same join keys. Record usage per attempt because retries consume resources, while recording the business result once per idempotent commit. This lets a tenant report show both successful call summaries and retry overhead without double-counting CRM actions. Keep money out of the hot-path decision: the worker's first duty is a correct, attributable transition, while budgets and rate controls operate at admission time.

## Migration and rollback proceed by tenant cohort

Before rollout, shadow the new taxonomy version without writing CRM actions. Compare acceptance rate, quarantine reasons, label-set changes, latency, and usage by tenant. No single aggregate should authorize promotion: a high-volume tenant can hide a smaller tenant whose calls all fail validation. The deployment decision should use tenant slices, and the dashboard should join queue age, attempts, request IDs, taxonomy versions, validation outcomes, and committed CRM action IDs. Promote by tenant cohort, beginning with accounts whose taxonomy and call patterns are represented in the evaluation corpus; this makes the cost delta and rejection delta attributable to the same population rather than to a changing traffic mix.

Rollback means stopping new admissions to the candidate model or taxonomy, draining or quarantining its queued jobs, and routing new work to the last accepted configuration. Do not rewrite already committed labels in place. If business owners require reclassification, enqueue a new versioned operation whose audit record points to the superseded result.

The catch is that an LLM classifier is not suitable when the label decision must be perfectly reproducible from stable rules, when the taxonomy is tiny and keyword logic is sufficient, or when the organization cannot retain enough metadata to audit a CRM action. Stick with deterministic rules for those paths. Embedding similarity can help retrieve candidate labels for a large taxonomy, but candidate retrieval does not replace exact membership validation; the final output still has to cross the same closed-set gate.

This design also trades some recall for containment. A valid but incomplete label set can pass structural checks, so sampled human review and a labeled evaluation corpus remain necessary. Your mileage may vary by catalog language and transcript quality. What shouldn't vary is the commit rule: no exact, versioned, attributable label set means no CRM mutation.

## References

- https://platform.openai.com/docs/guides/embeddings
- https://sharp.pixelplumbing.com
