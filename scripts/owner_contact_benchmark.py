"""Run a repeatable owner-contact benchmark on a small CSV subset.

The script intentionally creates benchmark-specific lead IDs so repeated runs
do not collide with real CSV imports that use the normal CSV_* dedup key.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.db_queries import create_run, get_leads_by_run_id, update_run
from pipeline.csv_import import (
    _coerce_value,
    _parse_city_state_zip,
    _parse_full_address,
    _sanitize_mapped_value,
    map_columns,
)
from pipeline.db import get_connection, insert_leads_csv_batch
from pipeline.run import run_csv_pipeline


DEFAULT_CSV = Path("/Users/neoudhaile/Desktop/simple tier/short.csv")
DEFAULT_OUTPUT_DIR = Path("docs/superpowers/plans/benchmarks")
NON_TRUTHFUL_SOURCES = {"", "none", "csv_import", "claude_inferred", "company_fallback"}
OWNER_FIELDS = ("owner_name", "owner_email", "owner_phone")


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug[:50] or "owner-contact-benchmark"


def _load_subset(csv_path: Path, limit: int) -> tuple[list[str], list[dict]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        headers = reader.fieldnames or []
    if limit > 0:
        rows = rows[:limit]
    if not headers or not rows:
        raise ValueError(f"No rows found in {csv_path}")
    return headers, rows


def _lead_dicts(headers: list[str], rows: list[dict], label: str) -> tuple[list[dict], dict]:
    mapping = map_columns(headers, rows[:3])
    prepared = []
    prefix = f"BENCH_{_slug(label)}"

    for index, row in enumerate(rows, start=1):
        lead = {
            "raw_data": json.dumps({"benchmark_label": label, "row": row}),
            "source": "benchmark_csv",
            "status": "new",
        }

        for csv_col, db_col in mapping.items():
            raw = row.get(csv_col)
            if raw is None or str(raw).strip() == "":
                continue

            if db_col == "city" and "," in str(raw):
                for key, value in _parse_city_state_zip(str(raw)).items():
                    lead.setdefault(key, value)
                continue

            if db_col == "address" and "," in str(raw):
                for key, value in _parse_full_address(str(raw)).items():
                    lead.setdefault(key, value)
                continue

            coerced = _coerce_value(db_col, raw)
            sanitized = _sanitize_mapped_value(db_col, coerced)
            if sanitized is not None:
                lead[db_col] = sanitized

        if not lead.get("company"):
            continue

        dedup_basis = "|".join(
            str(lead.get(key) or "").lower().strip()
            for key in ("company", "address", "city")
        )
        digest = hashlib.sha256(dedup_basis.encode()).hexdigest()[:12]
        lead["google_place_id"] = f"{prefix}_{index}_{digest}"
        prepared.append(lead)

    return prepared, mapping


def _cleanup_label(conn, label: str) -> dict:
    prefix = f"BENCH_{_slug(label)}_%"
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT run_id FROM smb_leads WHERE google_place_id LIKE %s AND run_id IS NOT NULL",
            (prefix,),
        )
        run_ids = [row[0] for row in cur.fetchall()]
        cur.execute("DELETE FROM smb_leads WHERE google_place_id LIKE %s", (prefix,))
        deleted_leads = cur.rowcount
        deleted_runs = 0
        if run_ids:
            cur.execute("DELETE FROM pipeline_runs WHERE id = ANY(%s)", (run_ids,))
            deleted_runs = cur.rowcount
        conn.commit()
        return {"deleted_leads": deleted_leads, "deleted_runs": deleted_runs, "run_ids": run_ids}
    finally:
        cur.close()


def _source_for(lead: dict, field: str) -> str:
    meta = lead.get("enrichment_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    info = meta.get(field) or {}
    return str(info.get("source") or info.get("provider") or "none").lower()


def _truthy_owner_field(lead: dict, field: str) -> bool:
    if not lead.get(field):
        return False
    return _source_for(lead, field) not in NON_TRUTHFUL_SOURCES


def _breakdown(leads: list[dict], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for lead in leads:
        if not lead.get(field):
            source = "empty"
        else:
            source = _source_for(lead, field)
        counts[source] = counts.get(source, 0) + 1
    return dict(sorted(counts.items()))


def _metrics_from_leads(run_id, inserted_count: int, events: list[dict], leads: list[dict]) -> dict:
    kept = [
        lead for lead in leads
        if lead.get("tier") in {"tier_1", "tier_2", "tier_3"} or lead.get("tier") is None
    ]
    out = {
        "run_id": run_id,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "inserted_count": inserted_count,
        "remaining_count": len(leads),
        "kept_count": len(kept),
        "event_counts": {},
        "fields": {},
    }
    for event in events:
        event_type = event.get("type", "unknown")
        out["event_counts"][event_type] = out["event_counts"].get(event_type, 0) + 1
    for field in OWNER_FIELDS:
        out["fields"][field] = {
            "all_nonempty": sum(1 for lead in leads if lead.get(field)),
            "all_truthful": sum(1 for lead in leads if _truthy_owner_field(lead, field)),
            "kept_nonempty": sum(1 for lead in kept if lead.get(field)),
            "kept_truthful": sum(1 for lead in kept if _truthy_owner_field(lead, field)),
            "source_breakdown": _breakdown(leads, field),
        }
    out["lead_diagnostics"] = _lead_diagnostics(leads, events)
    return out


def _compact_value(value):
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item)[:160] for item in value[:5]]
    return str(value)[:240]


def _lead_step_events(events: list[dict], lead_id) -> list[dict]:
    return [
        event for event in events
        if event.get("lead_id") == lead_id
        and event.get("type") in {"enrich_step_done", "enrich_step_skip", "enrich_step_error"}
    ]


def _missing_field_reason(field: str, lead: dict, step_events: list[dict]) -> str:
    value = lead.get(field)
    if value:
        source = _source_for(lead, field)
        if source in NON_TRUTHFUL_SOURCES:
            return f"{field} exists but source '{source}' is not counted as truthful owner contact."

    relevant_steps = {
        "owner_name": ("Website scrape", "Openmart company enrich", "Apollo", "Hunter.io", "Sixtyfour", "FullEnrich"),
        "owner_email": ("Openmart company enrich", "Apollo", "Hunter.io", "Sixtyfour", "FullEnrich", "Company fallback"),
        "owner_phone": ("Openmart company enrich", "Apollo", "Hunter.io", "Sixtyfour", "FullEnrich"),
    }[field]

    errors = [
        event for event in step_events
        if event.get("step") in relevant_steps and event.get("type") == "enrich_step_error"
    ]
    if errors:
        event = errors[-1]
        return f"{event.get('step')} errored: {event.get('error')}"

    done_without_field = [
        event for event in step_events
        if event.get("step") in relevant_steps
        and event.get("type") == "enrich_step_done"
        and field not in (event.get("fields_filled") or [])
    ]
    if done_without_field:
        steps = ", ".join(sorted({str(event.get("step")) for event in done_without_field}))
        return f"{steps} ran but did not return {field}."

    skips = [
        event for event in step_events
        if event.get("step") in relevant_steps
        and event.get("type") == "enrich_step_skip"
        and event.get("detail")
    ]
    if skips:
        event = skips[-1]
        return f"{event.get('step')} skipped: {event.get('detail')}"

    return f"No owner-contact provider produced {field}."


def _lead_diagnostics(leads: list[dict], events: list[dict]) -> list[dict]:
    diagnostics = []
    for lead in leads:
        lead_id = lead.get("id")
        step_events = _lead_step_events(events, lead_id)
        owner_fields = {}
        missing_truthful = []
        why_missing = {}
        for field in OWNER_FIELDS:
            source = _source_for(lead, field)
            truthful = _truthy_owner_field(lead, field)
            owner_fields[field] = {
                "value": _compact_value(lead.get(field)),
                "source": source,
                "truthful": truthful,
            }
            if not truthful:
                missing_truthful.append(field)
                why_missing[field] = _missing_field_reason(field, lead, step_events)

        diagnostics.append({
            "lead_id": lead_id,
            "company": lead.get("company"),
            "city": lead.get("city"),
            "state": lead.get("state"),
            "website": lead.get("website"),
            "tier": lead.get("tier"),
            "owner_fields": owner_fields,
            "missing_truthful": missing_truthful,
            "why_missing": why_missing,
            "step_outcomes": [
                {
                    "step": event.get("step"),
                    "type": event.get("type"),
                    "detail": event.get("detail") or event.get("error"),
                    "fields_filled": event.get("fields_filled") or [],
                    "field_sources": event.get("field_sources") or {},
                    "elapsed": event.get("elapsed"),
                }
                for event in step_events
            ],
        })
    return diagnostics


def _metrics(run_id: int, inserted_count: int, events: list[dict]) -> dict:
    conn = get_connection()
    try:
        leads = get_leads_by_run_id(conn, run_id)
    finally:
        conn.close()
    return _metrics_from_leads(run_id, inserted_count, events, leads)


def _write_metrics(metrics: dict, output_dir: Path, label: str, update_latest: bool = True) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = output_dir / f"{timestamp}-{_slug(label)}-run-{metrics['run_id']}.json"
    path.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n")
    if update_latest:
        latest = output_dir / f"latest-{_slug(label)}.json"
        latest.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n")
    return path


def _merge_update(target: dict, fields: dict):
    for key, value in fields.items():
        if key == "enrichment_meta" and isinstance(value, str):
            try:
                target[key] = json.loads(value)
            except json.JSONDecodeError:
                target[key] = value
        else:
            target[key] = value


def _run_in_memory_benchmark(args: argparse.Namespace, leads: list[dict], mapping: dict) -> dict:
    import pipeline.enrichment as enrichment

    run_id = f"memory-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    events: list[dict] = []
    lead_map = {}
    for index, lead in enumerate(leads, start=1):
        lead_copy = dict(lead)
        lead_copy["id"] = index
        lead_copy["run_id"] = None
        lead_map[index] = lead_copy

    def emit(event: dict):
        event = dict(event)
        event["run_id"] = run_id
        events.append(event)
        if args.verbose:
            print(json.dumps(event, sort_keys=True, default=str))

    original_get_lead = enrichment.get_lead
    original_update_lead = enrichment.update_lead
    original_get_by_place = enrichment.get_lead_by_google_place_id

    def fake_get_lead(lead_id: int):
        lead = lead_map.get(lead_id)
        return dict(lead) if lead else None

    def fake_update_lead(lead_id: int, fields: dict):
        if lead_id in lead_map:
            _merge_update(lead_map[lead_id], fields)

    try:
        enrichment.reset_x402_flag()
        enrichment.get_lead = fake_get_lead
        enrichment.update_lead = fake_update_lead
        enrichment.get_lead_by_google_place_id = lambda _place_id: None

        for lead_id in lead_map:
            emit({
                "type": "insert",
                "lead_id": lead_id,
                "company": lead_map[lead_id].get("company", ""),
                "industry": lead_map[lead_id].get("industry", ""),
                "city": lead_map[lead_id].get("city", ""),
            })
            enrichment.enrich_lead(lead_id, emit=emit)
    finally:
        enrichment.get_lead = original_get_lead
        enrichment.update_lead = original_update_lead
        enrichment.get_lead_by_google_place_id = original_get_by_place

    final_leads = [lead_map[key] for key in sorted(lead_map)]
    metrics = _metrics_from_leads(run_id, len(final_leads), events, final_leads)
    metrics["label"] = args.label
    metrics["csv"] = str(args.csv)
    metrics["limit"] = args.limit
    metrics["mapping"] = mapping
    metrics["mode"] = "memory"
    metrics["cleanup"] = {}
    metrics["status"] = "completed"
    metrics["error_message"] = None
    path = _write_metrics(metrics, args.output_dir, args.label)
    metrics["output_path"] = str(path)
    return metrics


def run_benchmark(args: argparse.Namespace) -> dict:
    headers, rows = _load_subset(args.csv, args.limit)
    leads, mapping = _lead_dicts(headers, rows, args.label)
    if not leads:
        raise ValueError("No importable leads in selected CSV subset")

    if args.cleanup_only:
        conn = get_connection()
        try:
            cleanup = _cleanup_label(conn, args.label)
        finally:
            conn.close()
        metrics = {
            "run_id": "cleanup-only",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "inserted_count": 0,
            "remaining_count": 0,
            "kept_count": 0,
            "event_counts": {},
            "fields": {
                "owner_name": {},
                "owner_email": {},
                "owner_phone": {},
            },
            "label": args.label,
            "csv": str(args.csv),
            "limit": args.limit,
            "mapping": mapping,
            "mode": "cleanup",
            "cleanup": cleanup,
            "status": "completed",
            "error_message": None,
        }
        path = _write_metrics(metrics, args.output_dir, args.label, update_latest=False)
        metrics["output_path"] = str(path)
        return metrics

    if args.mode == "memory":
        return _run_in_memory_benchmark(args, leads, mapping)

    conn = get_connection()
    try:
        cleanup = _cleanup_label(conn, args.label) if args.cleanup else {}
        run_id = create_run(conn)
        for lead in leads:
            lead["run_id"] = run_id
        inserted_map = insert_leads_csv_batch(conn, leads)
    finally:
        conn.close()

    lead_ids = [inserted_map[lead["google_place_id"]] for lead in leads if lead["google_place_id"] in inserted_map]
    events: list[dict] = []

    def emit(event: dict):
        event = dict(event)
        event["run_id"] = run_id
        events.append(event)
        if args.verbose:
            print(json.dumps(event, sort_keys=True, default=str))

    status = "completed"
    error_message = None
    try:
        run_csv_pipeline(lead_ids, emit=emit, run_id=run_id)
    except Exception as exc:  # keep failed runs measurable
        status = "failed"
        error_message = str(exc)
        emit({"type": "error", "message": error_message})
    finally:
        conn = get_connection()
        try:
            done = next((event for event in reversed(events) if event.get("type") == "done"), {})
            update_run(
                conn,
                run_id,
                status=status,
                finished_at=datetime.now(timezone.utc),
                inserted=done.get("inserted", len(lead_ids)),
                skipped_geo=0,
                skipped_dupe=0,
                total_leads=done.get("total_leads"),
                error_message=error_message,
            )
        finally:
            conn.close()

    metrics = _metrics(run_id, len(lead_ids), events)
    metrics["label"] = args.label
    metrics["csv"] = str(args.csv)
    metrics["limit"] = args.limit
    metrics["mapping"] = mapping
    metrics["mode"] = "db"
    metrics["cleanup"] = cleanup
    metrics["status"] = status
    metrics["error_message"] = error_message
    path = _write_metrics(metrics, args.output_dir, args.label)
    metrics["output_path"] = str(path)
    return metrics


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--label", default="owner-contact-short")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--mode",
        choices=("memory", "db"),
        default="memory",
        help="memory calls enrichment APIs without DB writes; db runs the full CSV pipeline.",
    )
    parser.add_argument("--no-cleanup", action="store_false", dest="cleanup")
    parser.add_argument("--cleanup-only", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    metrics = run_benchmark(args)
    summary = {
        "run_id": metrics["run_id"],
        "status": metrics["status"],
        "output_path": metrics["output_path"],
        "remaining_count": metrics["remaining_count"],
        "kept_count": metrics["kept_count"],
        "owner_name": metrics["fields"]["owner_name"],
        "owner_email": metrics["fields"]["owner_email"],
        "owner_phone": metrics["fields"]["owner_phone"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if metrics["status"] == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
