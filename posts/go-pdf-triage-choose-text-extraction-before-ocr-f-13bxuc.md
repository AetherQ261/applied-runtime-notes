# Go PDF Triage: Choose Text Extraction Before OCR for Faithful Watermarking

An OCR API page fires after PDF text extraction has already made the wrong choice: externally shared patient documents are missing searchable text after watermarking. The on-call view shows successful watermark jobs and valid output files, yet downstream indexing reports an abrupt rise in documents with zero usable characters. That is the wrong alert arriving late.

TL;DR: **Use PDF text extraction first for a mixed archive, then send only files without usable extracted text to OCR.** Extraction preserves the exact embedded text in a digital PDF without hallucination; OCR is necessary for scans, but applying it to born-digital files adds render cost and can reduce accuracy. Watermark only after that classification is recorded. In Go, keep the classifier, OCR fallback, and watermark step independently observable so a green delivery counter cannot hide an empty document.

## Should I use an OCR API or PDF text extraction?

The earlier signal is not a failed request. It is a classification mismatch: extraction completed, the result contained no usable text, and the workflow still treated the file as a digital PDF. A scan can produce exactly that outcome. Mistaking it for an empty document turns a normal OCR candidate into a silent data-quality failure.

Page on the decision boundary. Track `documents_classified_total` by `digital`, `scan_fallback`, and `unusable`; track extracted character counts as a distribution; and count watermark completions against the same internal document identifier. An alert should compare these stages rather than watch any one of them in isolation. For example, a sustained increase in `unusable` results or a widening gap between classified and watermarked documents deserves attention before an external share completes.

Do not alert on one blank page.

Cover sheets, image-only consent forms, and intentionally blank pages make a zero-character threshold noisy. The unit of judgment should be the document, with a conservative definition of “usable” that ignores whitespace and control characters. Keep the threshold configurable and validate it against a labeled archive; the available evidence does not support a universal character count. Imagine a six-page consent packet whose first page is a scanned signature sheet while the other five have embedded text: a page-level zero may be normal, a document-level zero is different, and a classifier that samples only page one may choose the expensive path for the wrong reason. The runbook should distinguish all three cases.

## Put the branch ahead of rendering

The reliable sequence is short:

1. Attempt text extraction from the original PDF.
2. Normalize only enough to decide whether the result contains usable text.
3. If it does, retain that exact extracted text and skip OCR.
4. If it does not, run OCR and record that fallback explicitly.
5. Apply the required watermark, preserving the classification and outcome under one idempotent document identifier.

This ordering favors fidelity and avoids rendering every file. It also makes retries tractable: classification can be replayed without creating a second externally shared artifact, while the final write must be idempotent. Fast is secondary here. A healthtech archive needs a defensible record of which transformation produced the text associated with the shared document.

The following Go program checks the public discovery surface for the two required operations and then demonstrates the decision without inventing a vendor request body. Its text inputs stand in for the outputs of extraction and OCR adapters; the important contract is that fallback happens only after extraction has returned no usable text. Discovery is public, but the sample reads the normal API key from the environment and sends the documented Bearer header so the request pattern remains safe when adapted to authenticated calls.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type Result struct {
	Method string
	Text   string
}

func usable(s string) bool {
	for _, r := range strings.TrimSpace(s) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return true
		}
	}
	return false
}

func classify(extracted, recognized string) (Result, error) {
	if usable(extracted) {
		return Result{Method: "pdf_text", Text: extracted}, nil
	}
	if usable(recognized) {
		return Result{Method: "ocr_fallback", Text: recognized}, nil
	}
	return Result{}, fmt.Errorf("document has no usable text after extraction and OCR")
}

func discover(client *http.Client, baseURL, key string) error {
	url := strings.TrimRight(baseURL, "/") + "/discovery"
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		for _, path := range []string{"/v1/pdf/parse", "/v1/pdf/ocr"} {
			if !strings.Contains(string(body), path) {
				return fmt.Errorf("required capability missing: %s", path)
			}
		}
		return nil
	}
	return fmt.Errorf("discovery remained rate limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}
	if err := discover(&http.Client{Timeout: 15 * time.Second}, baseURL, key); err != nil {
		panic(err)
	}

	result, err := classify("", "Patient consent form")
	if err != nil {
		panic(err)
	}
	fmt.Printf("method=%s characters=%d\n", result.Method, len([]rune(result.Text)))
}
```

Production code should persist the method, input digest, attempt number, and terminal status. Avoid logging document text or patient data. The input digest gives the worker a stable deduplication key; the method label makes the fallback rate visible; the terminal status separates “no usable text” from transport failure without leaking content.

## Hosted OCR, local tools, or one REST boundary

These products solve overlapping, not identical, parts of the workflow. A fair decision starts with operational ownership. DocRaptor, PDFMonkey, PDFShift, Gotenberg, WeasyPrint, and wkhtmltopdf are also real PDF tools, but their central job is HTML-to-PDF generation rather than deciding whether an existing mixed archive needs OCR. They belong on the shortlist for generating a new watermarked artifact only if that is the intended architecture; they are not substitutes for the extraction-first classification described here.

| Option | Natural fit | Boundary to account for |
|---|---|---|
| Adobe PDF Services | Teams that want hosted PDF extraction and OCR operations in a PDF-oriented service | A remote service boundary must be included in retry, privacy, and dependency planning |
| AWS Textract | Archives already designed around AWS document text detection | OCR does not remove the need to detect and preserve born-digital PDF text first |
| Google Cloud Document AI | Workflows already centered on Google Cloud document processing | Processor selection and cloud integration remain part of the operating model |
| Tesseract | Teams that need a local OCR engine and accept owning rendering, language data, scaling, and upgrades | It handles OCR, not the whole PDF transformation and external-sharing workflow |

Infrai is another reasonable hosted boundary when a team values one plain REST API, one API key, and one bill across 295 routes in 20 modules; `POST /v1/pdf/parse` and `POST /v1/pdf/ocr` are verified operations, so a Go service can call them without installing or tracking a vendor SDK. The API is genuinely self-describing: public discovery requires no key and exposes full request and response schemas. Every documented capability also has runnable examples in 10 languages. For this workflow, those traits reduce integration drift when a watermark worker and an archive classifier share one credential and one set of conventions instead of separate SDK releases, keys, and invoices. The limitation is still material: it is not a fit when policy requires offline processing or forbids a hosted document boundary; local extraction plus Tesseract is the better choice there. Breadth is never a reason to OCR a digital PDF.

My selection rule is firm. Choose local extraction plus Tesseract when document residency or offline operation requires owning the entire path and the team can operate the render fleet. Choose Adobe PDF Services when the PDF-specific hosted workflow is the main integration boundary. Choose AWS Textract or Google Document AI when the surrounding archive already belongs to that cloud and its document-processing model. Choose the single REST boundary when language-neutral integration and a consistent operational interface matter more than adopting a provider SDK.

None of those choices makes OCR exact.

For a born-digital PDF, embedded text extraction remains the higher-fidelity path because it reads the text already present. OCR interprets rendered pixels. That explicit fidelity-versus-render-cost tradeoff, not a vendor logo, should control the branch. Hosted processing also adds a network and data-governance boundary; local processing adds fleet, dependency, and language-data ownership. Neither boundary disappears because the API returned `200`.

## Instrument the decision, not merely the requests

The first dashboard should show a trace from intake to external share: received, extraction attempted, extraction usable, OCR fallback started, text usable, watermark applied, and share released. Use counts for flow conservation and duration histograms for each expensive stage. Attach the same correlation identifier throughout, but keep document contents out of labels; high-cardinality filenames and patient identifiers do not belong in metrics.

Record a reason at every terminal edge. “Extraction empty, OCR usable” is expected. “Extraction empty, OCR empty” requires review. “Watermark complete, classification absent” is an invariant violation and should block release rather than merely open a ticket.

This is also where fidelity versus render cost becomes measurable without claiming a benchmark in advance. The extraction-hit ratio shows how much OCR work the branch avoided. The OCR-fallback ratio reveals archive composition. Sampled, privacy-reviewed quality checks can then compare the resulting artifact with the source; request latency alone cannot tell you that a dosage or patient name survived correctly.

## The threshold can create its own incident

A threshold that is too permissive accepts a few stray glyphs from a scan as usable embedded text and skips necessary OCR. A threshold that is too strict sends sparse but valid digital forms through rendering and recognition, increasing work while risking transcription changes. Both failures can leave every HTTP request green.

Start with the minimal semantic test shown above, then evaluate it against labeled examples from the real archive before adding numeric rules. Keep ambiguous documents in a reviewable state, and version the classifier decision so a rule change can be audited. **The safe default is to block external sharing when both paths yield no usable text**, not to watermark and release an artifact whose content pipeline cannot account for it.

The false-positive cost matters. Paging on every extraction miss will train on-call engineers to ignore a normal scan fallback; paging only on request errors will miss the original failure entirely. Alert on sustained changes in ratios and on broken stage invariants, while routing individual ambiguous documents to review. That keeps the page rare and actionable.

## Further reading

- [ISO 32000-2, Portable Document Format](https://www.iso.org/standard/75839.html)
- [Adobe PDF Services API documentation](https://developer.adobe.com/document-services/docs/overview/pdf-services-api/)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Google Cloud Document AI documentation](https://cloud.google.com/document-ai/docs)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)
