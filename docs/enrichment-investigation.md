# Enrichment Pipeline — Bug Investigation

Tracking enrichment funnel issues observed in runs 53 and 54.

## Summary

Out of 19 leads, only 4 get any enrichment. The other 15 exit the waterfall completely empty — no website, no contacts, no services, nothing. The root cause is a cascade failure: Google Places can't match them, so every downstream step skips.

---

## Bug 1: Google Places fails to match 15/19 leads

**Status:** Investigating

**Observed:** Leads like "Palmdale Water District" at "2029 East Avenue Q, Palmdale, California" have both a company name and address, but Google Places returns no match.

**Root cause analysis:**
- `find_place()` in `google_places.py` constructs a search query from company + address, calls the Google Places Text Search API, then scores each candidate using `_candidate_score()`.
- The scoring requires BOTH a name similarity >= 0.68 AND an address score >= 0.55 (when address is provided).
- `_normalize_text()` strips stopwords including `district`, `water`, `municipal`, `services`, `department` — so "Palmdale Water District" becomes just "palmdale". The Google result's display name would also be normalized, but if Google returns "Palmdale Water District" vs our "Palmdale Water District" the similarity should be high.
- **Likely issue:** The Google Places search itself may return zero candidates for these queries. The `minRating` filter in `search_text()` requires a minimum rating — companies without any reviews/rating would be excluded by Google. OR the `locationRestriction` bounding box around LA is filtering out companies in Palmdale, Temecula, etc. if they're outside the configured radius.
- **Also possible:** The `_normalize_text` stopword stripping is too aggressive — after removing "water", "district", "municipal", "services", etc., what's left may not match anything.

**Root causes found (2026-04-20):**
1. `search_text()` passes `minRating: 3.5` to Google — companies with no ratings are excluded entirely.
2. `search_text()` uses `locationRestriction` with a 40-mile box from downtown LA — Temecula (~85mi), Palmdale (~60mi), Murrieta (~80mi) are all outside.
3. `_normalize_text()` strips `district`, `water`, `municipal`, `services`, `department` — "Palmdale Water District" becomes just "palmdale", killing name similarity scoring.

**Fix applied:**
- Added `_search_for_enrichment()` — calls Google Places Text Search without `minRating` or `locationRestriction` filters. Used by `find_place()` during enrichment.
- Reduced `_normalize_text()` stopwords to only corporate suffixes (inc, llc, ltd, co, company, corp, corporation). Domain-meaningful words like water, district, municipal now preserved.

---

## Bug 2: Claude failsafe skips leads that have many empty fields

**Status:** Fixed (2026-04-21)

**Observed:** For the 15 unenriched leads, Claude failsafe reports "No missing non-contact fields remained to infer" — but these leads have empty `services_offered`, `company_description`, `review_summary`, `certifications`, etc.

**Root cause analysis:**
- `_step_claude_failsafe()` calls `_has_grounded_evidence()` first — if this returns `False`, the failsafe bails out immediately (returns 0.0).
- `_has_grounded_evidence()` requires either a real Google place ID or at least one "grounded" field (website, company_phone, company_email, owner_name, owner_email, services_offered, company_description) from a non-CSV, non-Claude source.
- For the 15 unenriched leads, there's NO grounded evidence — no Google match, no website, nothing from any API. All their data came from Openmart/CSV import.
- This is by design — the guard prevents Claude from hallucinating when there's nothing real to anchor inferences to. But it means that if Google Places fails, Claude failsafe also fails, and the lead stays completely empty.

**The skip reason message is misleading.** The actual skip is `_has_grounded_evidence() == False`, but the emitted message says "No missing non-contact fields remained to infer" (which is the generic `STEP_SKIP_REASONS` string, not the actual reason).

**Fix applied (2026-04-21):**
- Relaxed the guard in `_step_claude_failsafe()`: the failsafe now runs if the lead has EITHER grounded API evidence OR minimal identity (company name + city or address). This lets Claude still infer non-contact fields like `services_offered`, `company_description`, `industry` when Google Places fails to match.
- Kept the hard guardrail: the failsafe still cannot write owner contact fields (enforced by `CLAUDE_FAILSAFE_FIELDS` allowlist), so Claude cannot hallucinate names/emails/phones even on identity-only leads.
- Loosened the LOW_VALUE_CLAUDE_FIELDS gate: it only applies when we already have grounded evidence. Identity-only leads may attempt even low-value fields because the marginal cost is small and the lead has no other data.
- Replaced the single static skip message with three specific dynamic reasons: "no evidence and no identity", "all fields already populated", and "only low-value fields remained". Reasons are written to `meta['__skip_reason']` and picked up by `_run_step` when emitting the skip event.
- Updated `tests/test_claude_failsafe_scope.py` to match the new behavior (identity-only leads now DO call Claude).

---

## Bug 3: Employee count is 126 for nearly every company

**Status:** Investigating

**Observed:** Companies ranging from tree services to municipal water districts to energy startups all show `employee_count: 126`. This is clearly wrong.

**Root cause analysis:**
- This data comes from Openmart (the source API). It's either a default/placeholder value Openmart returns when it doesn't have real employee data, or a parsing bug in how we read Openmart's response.

**Next steps:**
1. Check `normalize.py` and `scraper.py` to see how `employee_count` is extracted from Openmart data.
2. Check `raw_data` JSONB in the DB for one of these leads to see what Openmart actually returns.

---

## Bug 4: No outreach emails generated for any lead

**Status:** Investigating

**Observed:** All 19 leads have blank `generated_subject` and `generated_email`, including fully enriched ones like Farwest Corrosion (Tier 2) and Cucamonga Valley Water District.

**Next steps:**
1. Check `run.py` to see if email generation step is being called.
2. Check if there's a tier gate or other condition preventing generation.

---

## Change Log

| Date | Change |
|------|--------|
| 2026-04-20 | Initial investigation doc created. 4 bugs identified from runs 53/54. |
| 2026-04-20 | Bug 1 fix: added unfiltered search for enrichment, reduced stopwords. |
| 2026-04-21 | Bug 2 fix: failsafe guard now accepts identity-only leads; dynamic skip reasons emitted. |
