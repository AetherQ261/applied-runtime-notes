# Live DNS Auditing Records Show Who Changed Mail (and Why Cutovers Wait)

TL;DR. Put the authoritative change record in an append-only audit stream owned by the change system, then copy the event into the operational log store for search and alerting. For a media publisher rolling out SPF, DKIM, and DMARC, that split preserves who changed what even while DNS caches and receivers see different states. Choose the cutover window from observed evidence, not from the instant an API reports success.

The useful audit event is a fact about an attempted change, not a snapshot taken later. It should include the actor, authorization context, zone and owner name, record type, old and requested values, request identifier, validation result, and timestamps. The log search index is a working copy. It is not the only evidence.

## How should DNS change auditing record who changed what in live logs?

The question is not “which DNS value is visible right now?” It is “which authorized action produced this value, and what did the system know at that moment?” Those are different questions during propagation.

DMARC supplies a good boundary for this model. RFC 7489 defines a policy record at `_dmarc` followed by a domain name and describes feedback reports sent to addresses in the `rua` and `ruf` tags. A change event therefore needs the complete record value, not a friendly label such as “DMARC enabled.” A reviewer must be able to distinguish a policy change from a reporting-address change.

For each accepted request, record an immutable event before treating the deployment as complete. Keep the before and after values as structured fields as well as a canonical serialized form. Normalize only for comparison; preserve the submitted representation separately when the organization needs to explain exactly what an operator requested.

One event, one owner.

The event should point to the change request and its authorization decision. It should not contain a secret private key. DKIM material is especially easy to mishandle because a DNS publication workflow deals with a public record while signing infrastructure holds private material. An audit trail that copies every payload into a broad log index creates a new exposure without improving the DNS evidence.

## Why does propagation change the cutover decision?

An authoritative update and a receiver’s observation are separate signals. A successful write proves that the change system accepted an update. It does not prove that every recursive resolver, mailbox provider, or message path will answer with the new value at the same time.

That distinction matters for a media operation with a scheduled newsletter. A rushed cutover can leave some recipients evaluating the old SPF or DMARC policy while other recipients evaluate the new one. The audit event must retain the write time and request identity; verification records should separately retain query time, vantage point, observed answer, and resolver behavior. Mixing these into one “status” field hides the exact failure mode.

I use a two-clock runbook for this kind of release. The first clock starts when the authorized change is accepted. The second starts when independent observations show the expected record at the required locations. The gap is the propagation budget. It is a reason to delay an enforcement step, not a reason to rewrite the original audit event.

## A small event contract that survives retries

Retries are normal in queue-driven operations. The event contract must make a repeated delivery harmless. Use a stable request ID and an idempotency key derived from the intended change; a retry may add an attempt record, but it must not create two claims that two different people made the same change.

Here is a deliberately generic Go shape for the durable event boundary. It does not assume a DNS provider, a log vendor, or a particular database.

```go
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

type ChangeEvent struct {
	RequestID       string    `json:"request_id"`
	IdempotencyKey  string    `json:"idempotency_key"`
	Actor           string    `json:"actor"`
	Authorization   string    `json:"authorization"`
	Zone            string    `json:"zone"`
	Owner           string    `json:"owner"`
	RecordType      string    `json:"record_type"`
	PreviousValue   string    `json:"previous_value"`
	RequestedValue  string    `json:"requested_value"`
	RequestedAt     time.Time `json:"requested_at"`
	Validation      string    `json:"validation"`
}

func (e ChangeEvent) Idempotency() string {
	b, _ := json.Marshal(struct {
		Zone, Owner, RecordType, RequestedValue string
	}{e.Zone, e.Owner, e.RecordType, e.RequestedValue})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
```

The example only computes a deterministic key. The real boundary still needs an atomic uniqueness check in the durable event store and a result that distinguishes “accepted,” “already recorded,” and “rejected.” That distinction belongs in the runbook because a caller that treats all three as success can report a false deployment.

## Where should live logs and evidence diverge?

Keep the source event in a system with controlled writes and retention appropriate to audit and compliance. Feed a derived representation to the live log platform, where responders can search by actor, request ID, domain, and record type. Give the search copy enough metadata to link back to the source event, including a content digest or equivalent immutable identifier.

The trade-off is deliberate:

| Location | Good at | Boundary |
| --- | --- | --- |
| Durable event store | Preserving the accepted change and its authorization link | Slower, narrower operational search |
| Live log index | Searching, alerting, and correlating a mail incident | Not necessarily the compliance record |
| Resolver observations | Showing what a recipient-side lookup saw | Cannot prove who submitted the change |

No single location answers all three questions.

This is a useful operational compromise. Search systems are excellent for a page during a failed send, but their indexing, retention, and access model may differ from the organization’s evidence requirements. Treating the search index as authoritative also makes a temporary ingestion outage look like missing history. The change should remain auditable even if the alerting pipeline is unavailable. That durability costs storage and an extra write path, so a small team may reasonably keep the event schema minimal while preserving the actor, authorization, old value, new value, and request ID.

Access is part of the record. Log reads should themselves be attributable, and sensitive values should be redacted according to policy. A reviewer needs to know that a value was changed and by whom; every downstream consumer does not need unrestricted access to every record payload. Retention should be explicit, tested, and consistent with the organization’s compliance obligation.

## Verification, rollback, and the stop condition

Before the mail cutover, verify the intended records from more than one independent resolver or observation point. Record the query timestamp and answer alongside the event ID. For DMARC, check that the `_dmarc` owner name and complete policy value are the ones approved for the release. Also check that report destinations are intentional; RFC 7489 treats aggregate and forensic reporting as distinct mechanisms, so a change to one should not be silently described as a change to the other.

Do not delete or edit the original event when rolling back. Submit a new authorized change whose previous value is the value currently observed or the last accepted state, then link it to the incident or release. This preserves the sequence that a compliance reviewer actually needs: request, decision, accepted update, observations, and reversal.

The stop condition is evidence-based: pause the enforcement step when observations disagree beyond the approved propagation window, when the actor or authorization link is absent, or when the durable event cannot be written. A green response from a write endpoint is not enough. That small rule prevents the most expensive kind of ambiguity: a mail delivery problem with no trustworthy account of what changed.

## References

- https://datatracker.ietf.org/doc/html/rfc7489
