# Open Graph Share Image Generation — Capacity Planning for Commerce Derivatives

Use an Open Graph share image generation API during the article publish job, compress the card once, and store it under an immutable content-derived key before the article becomes visible. The deciding constraint is not render speed. It is the combined storage and cache footprint of every retained article revision, image format, and size.

Short answer: treat a social card as a versioned e-commerce derivative, much like a product thumbnail. Build one supported fallback format at publish time, verify the bytes and metadata, then commit the article pointer. Do not render on crawler requests. A crawler retry should read the same object, not enqueue fresh image work.

This approach answers the practical API question without tying the publishing path to a particular rendering product or JavaScript runtime. The API boundary can be remote or in-process; the operational contract stays the same.

## How should an Open Graph share image generation API handle capacity?

A catalog article may feature a product image that is already large enough for the storefront. The share card is a separate derivative: fixed canvas, composed text, cropped merchandise, and compression chosen for distribution. Keeping it separate prevents a social preview change from rewriting the source asset.

Start with retained bytes, not request counts. For `A` published articles, `R` retained revisions per article, `F` encoded formats, and average encoded size `S`, the steady-state object footprint is approximately `A x R x F x S`, before replication and storage-system overhead. A planning set of 10,000 articles, 3 retained revisions, 1 format, and a measured 180 KB mean would represent about 5.4 GB of card payload before those overheads; these are example inputs, not a benchmark. Replace every one with measurements from the actual catalog. Text-heavy cards, detailed apparel, and flat-color backgrounds compress differently, so borrowing a generic web-image average hides the variable that matters most.

Measure first.

The dangerous multiplier is revision retention. If an editor republishes a product guide 12 times and every revision receives a new object, a policy that was cheap at launch can quietly become the dominant asset class. The runbook needs an explicit retention decision: preserve every referenced revision, or garbage-collect objects only after no article version and no rollback window points to them. That is a real trade-off. Longer retention makes rollback safer, while shorter retention limits storage growth; the acceptable window depends on editorial rollback policy rather than an encoder feature.

One format may be the right first move. MDN describes JPEG as a good choice for lossy compression of still images, while PNG provides lossless compression and supports transparency. WebP supports lossy and lossless compression, and AVIF offers strong compression but has different feature and browser-history considerations. Format selection is therefore a measured compatibility decision, not a file-extension preference. For a crawler-facing card, validate the actual consumers you support and retain a conservative fallback when that evidence calls for one.

## Why does request-time rendering fail under crawler retries?

Request-time rendering couples an uncontrolled fetch pattern to CPU, fonts, source-image availability, and the rendering dependency. The Open Graph protocol requires `og:image` to identify an image URL; it does not require that URL to perform fresh composition on every request. A stable object is the smaller failure domain.

Retries are normal input, not an exceptional state. If 10 fetches for the same article can create 10 renders, the endpoint lacks the idempotency property the workload needs. Cache misses then become work amplification: the moment an article attracts attention is also the moment rendering receives the most duplicate demand.

Publish-time generation moves that demand to a controlled queue. It also makes failure legible. The article revision is either ready with a verified image key, or it is not ready. There is no ambiguous state where the page is public but its card may render differently depending on which request arrived first. This approach is a poor fit for cards whose pixels must reflect live inventory or a rapidly changing price at fetch time; in that case, bounded request-time rendering with request coalescing and a stale fallback is the more honest design. It is also unnecessary for a tiny, rarely edited catalog where a checked-in static asset already meets the publishing contract.

No queue is free.

## Use a revision manifest as the commit boundary

The safe flow has 4 states: render, verify, store, commit. Render from an immutable input snapshot. Verify the encoded output before upload. Store it under a key derived from every input that can change pixels. Commit the article manifest last.

The key should include the template version, normalized article fields, source-image digest, dimensions, and encoder settings. A changed quality value must produce a changed key. So must a font or crop-rule change. This makes retries cheap and prevents stale output from masquerading as current output.

```go
package cards

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
)

type Spec struct {
    ArticleID    string
    Revision     string
    Template     string
    SourceDigest string
    Width        int
    Height       int
    Codec        string
    Quality      int
}

type Store interface {
    Exists(ctx context.Context, key string) (bool, error)
    PutIfAbsent(ctx context.Context, key, contentType string, body []byte) error
}

func ObjectKey(s Spec) string {
    canonical := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d\x00%d\x00%s\x00%d",
        s.ArticleID, s.Revision, s.Template, s.SourceDigest,
        s.Width, s.Height, s.Codec, s.Quality)
    sum := sha256.Sum256([]byte(canonical))
    return "social-cards/" + hex.EncodeToString(sum[:]) + "." + s.Codec
}

func Materialize(ctx context.Context, store Store, spec Spec, encoded []byte) (string, error) {
    key := ObjectKey(spec)
    ok, err := store.Exists(ctx, key)
    if err != nil {
        return "", fmt.Errorf("check card: %w", err)
    }
    if ok {
        return key, nil
    }
    if len(encoded) == 0 {
        return "", fmt.Errorf("reject empty card")
    }
    if err := store.PutIfAbsent(ctx, key, "image/jpeg", encoded); err != nil {
        return "", fmt.Errorf("store card: %w", err)
    }
    return key, nil
}
```

The example deliberately leaves rendering behind an interface boundary. The storage operation is conditional, so two workers racing on the same revision converge on one object. In production, verify that the declared codec agrees with the bytes and content type; the fixed `image/jpeg` value shown here is correct only when the accepted spec is restricted to JPEG. If multiple codecs are allowed, map each accepted codec to its registered media type and reject unknown values.

Commit a small manifest after upload: article revision, object key, content type, pixel dimensions, byte length, and source digest. The public page reads only committed manifests. **Publishing is the transaction boundary**, even when the object store and article database cannot share a transaction. A failed render leaves an unreferenced object that a later sweeper may remove; it does not leave a public article pointing at missing media.

## Set budgets from measured derivatives

Storage cost and cache cost pull in different directions. More variants can reduce bytes transferred to particular clients, but every additional variant increases stored objects, invalidation work, and verification scope. For social crawlers, a single validated rendition often has a clearer operational envelope than a matrix of widths. Add a format or dimension only after request evidence identifies a consumer that benefits.

Track byte-weighted outcomes, not just job counts. The useful publish metrics are encoded bytes by template and codec, compression ratio against the rendered intermediate, render duration, queue age, deduplication hits, verification failures, and committed cards without a successful read probe. A daily histogram of encoded size will expose a template change that doubles storage long before the monthly bill does.

Set two budgets. The per-object budget rejects accidental oversized cards during verification. The fleet budget estimates retained bytes over the revision window and alerts on slope, because a gradual leak in retention will not trip a per-object check. **Capacity is a release criterion.** If a new template exceeds its measured byte budget, keep the previous manifest active while the team adjusts layout, crop, or encoder settings.

Avoid treating compression quality as a universal constant. Build a review corpus that contains fine product textures, small lettering, high-contrast sale labels, and flat backgrounds. Compare candidate encodes at the dimensions actually published. Visual acceptance and encoded size both matter; a tiny card that damages a product image is not an optimization.

## Verification and rollback runbook

Before exposing a revision, decode the stored object, confirm its media type, dimensions, nonzero length, and expected object key, then perform a read through the same public delivery path used by crawlers. Confirm that the page's `og:image` value is an absolute URL and that it resolves to the committed object. The Open Graph protocol also defines structured image properties including secure URL, type, width, height, and alternative text; emit the ones your page can state accurately.

Test idempotency directly: submit the same publish revision twice and require the second run to reuse the key without increasing the object count. Then change exactly 1 pixel-affecting input and require a different key. This 2-case test catches both duplicate creation and under-specified cache keys.

Rollback is pointer movement. Keep the last known-good manifest through the rollback window, and switch the article revision back to that manifest if verification or post-publish probes fail. Do not overwrite an object in place. Immutable URLs let caches retain correct old bytes while the page changes which revision it references.

The operational order is short:

1. Pause commits for the affected template version, while allowing unrelated templates to publish.
2. Keep or restore the previous verified manifest.
3. Quarantine newly generated objects from future commits; do not delete objects still referenced by any revision.
4. Reproduce with the immutable input snapshot, correct the template or encoder configuration, and publish under a new key.
5. Resume commits only after duplicate-submit and public-read checks pass.

Do not purge first. A broad purge removes the known-good fallback and turns a contained generation fault into a delivery incident. The recovery target is a valid prior pointer, not an empty cache.

A publish-time card pipeline is successful when article visibility, derivative readiness, and retained capacity share one explicit contract. Render APIs can change behind that contract. The durable parts are the content-derived key, conditional write, verified manifest, bounded retention, and pointer-based rollback.

## References

- https://ogp.me/
- https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types
- https://www.rfc-editor.org/rfc/rfc9111
- https://www.iana.org/assignments/media-types/media-types.xhtml
