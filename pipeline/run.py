import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from pipeline.config import (
    CITIES,
    ENRICH_CONCURRENCY,
    INDUSTRIES,
    MAX_LEADS_PER_RUN,
    MIN_RATING,
    MIN_REVIEWS,
)
from pipeline.db import (
    get_connection,
    insert_lead,
    count_leads,
    get_leads_by_ids,
    update_run_cost,
)
from pipeline.scraper import search_businesses, API_PAGE_SIZE
from pipeline.normalize import normalize_lead
from pipeline.enrichment import enrich_lead, check_x402_balance, reset_x402_flag
from pipeline.notion import export_leads_to_notion
from pipeline.tiering import tier_leads


def _checkpoint(wait_if_paused):
    if wait_if_paused is not None:
        wait_if_paused()


def _emit_non_tier1_skips(leads, emit):
    tier1_ids = []
    for lead in leads:
        if lead.get('tier') == 'tier_1':
            tier1_ids.append(lead['id'])
            continue
        emit({
            'type': 'tier_skip',
            'lead_id': lead.get('id'),
            'company': lead.get('company', ''),
            'tier': lead.get('tier'),
            'tier_reason': lead.get('tier_reason'),
            'message': 'Skipping non-Tier 1 lead before enrichment.',
        })
    return tier1_ids


def _retier_after_enrichment(lead_ids, emit=None, wait_if_paused=None):
    if not lead_ids:
        return []
    if emit is None:
        emit = lambda e: None
    tier_result = tier_leads(lead_ids, emit=emit, wait_if_paused=wait_if_paused)
    return tier_result['kept_ids']


def _run_stage_concurrently(
    lead_ids,
    worker_fn,
    on_success,
    on_error,
    concurrency,
    wait_if_paused=None,
):
    if not lead_ids:
        return 0.0

    total_cost = 0.0
    completed = 0
    lead_iter = iter(lead_ids)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as stage_executor:
        futures = {}

        def submit_next():
            _checkpoint(wait_if_paused)
            try:
                lead_id = next(lead_iter)
            except StopIteration:
                return False
            futures[stage_executor.submit(worker_fn, lead_id)] = lead_id
            return True

        for _ in range(min(max(1, concurrency), len(lead_ids))):
            if not submit_next():
                break

        while futures:
            done, _ = wait(list(futures.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                lead_id = futures.pop(future)
                completed += 1
                try:
                    result = future.result()
                    total_cost += float(result.get('cost', 0.0))
                    on_success(lead_id, result, completed, len(lead_ids))
                except Exception as e:
                    on_error(lead_id, e, completed, len(lead_ids))
                submit_next()

    return total_cost


def run_pipeline(emit=None, run_id=None, wait_if_paused=None):
    if emit is None:
        emit = lambda e: None
    reset_x402_flag()

    conn = get_connection()
    inserted_total = 0
    skipped_geo = 0
    skipped_dupe = 0
    total_queries = len(INDUSTRIES) * len(CITIES)
    inserted_ids: list[int] = []
    inserted_leads: list[dict] = []
    capped = False

    emit({'type': 'start', 'industries': INDUSTRIES, 'cities': CITIES,
          'total_queries': total_queries})

    try:
        # ── Search stage ───────────────────────────────────────────────────
        for idx, (industry, city) in enumerate(
            (i, c) for i in INDUSTRIES for c in CITIES
        ):
            if capped:
                break

            query_num = idx + 1
            cursor = None
            page_num = 0

            # Paginate this query until exhausted or capped
            while True:
                _checkpoint(wait_if_paused)
                page_num += 1

                emit({'type': 'search', 'query': industry, 'city': city,
                      'index': query_num, 'total': total_queries})

                results, next_cursor = search_businesses(
                    query=industry,
                    city=city,
                    page_size=API_PAGE_SIZE,
                    min_rating=MIN_RATING,
                    min_reviews=MIN_REVIEWS,
                    cursor=cursor,
                )

                if not results:
                    break

                emit({'type': 'results', 'query': industry, 'city': city,
                      'count': len(results)})

                emit({'type': 'geo', 'query': industry, 'city': city})

                batch_inserted = 0
                batch_rejected = 0

                for record in results:
                    _checkpoint(wait_if_paused)
                    lead = normalize_lead(record, industry)

                    if lead is None:
                        skipped_geo += 1
                        batch_rejected += 1
                        continue

                    if run_id is not None:
                        lead['run_id'] = run_id
                    lead_id = insert_lead(conn, lead)
                    if lead_id is not None:
                        inserted_total += 1
                        batch_inserted += 1
                        inserted_ids.append(lead_id)
                        inserted_leads.append({'id': lead_id, **lead})
                        emit({
                            'type': 'insert',
                            'lead_id': lead_id,
                            'company': lead['company'],
                            'industry': industry,
                            'city': city,
                            'distance_miles': lead['distance_miles'],
                            'ownership_type': lead['ownership_type'],
                        })
                        if len(inserted_ids) >= MAX_LEADS_PER_RUN:
                            emit({'type': 'search_capped', 'count': MAX_LEADS_PER_RUN})
                            capped = True
                            break
                    else:
                        skipped_dupe += 1
                        emit({'type': 'skip_dupe', 'company': lead['company']})

                emit({
                    'type': 'progress',
                    'query': industry,
                    'city': city,
                    'index': query_num,
                    'total': total_queries,
                    'inserted_so_far': inserted_total,
                    'skipped_geo_so_far': skipped_geo,
                    'skipped_dupe_so_far': skipped_dupe,
                    'batch_inserted': batch_inserted,
                    'batch_rejected': batch_rejected,
                })

                time.sleep(0.5)

                # Stop paginating if capped, no more pages, or page yielded nothing new
                if capped or next_cursor is None or batch_inserted == 0:
                    break
                cursor = next_cursor

        # ── Tier stage ─────────────────────────────────────────────────────
        tier_result = tier_leads(inserted_ids, emit=emit, wait_if_paused=wait_if_paused)
        tiered_leads = get_leads_by_ids(tier_result['kept_ids'])
        # Enrich all tiers, not just tier_1
        # inserted_ids = _emit_non_tier1_skips(tiered_leads, emit)
        inserted_ids = [lead['id'] for lead in tiered_leads]
        inserted_leads = tiered_leads
        inserted_total = len(inserted_ids)

        # ── Enrich stage ───────────────────────────────────────────────────
        total_cost = 0.0

        # ── Pre-flight balance check ──────────────────────────────────────
        estimated_cost = len(inserted_ids) * 0.003
        if estimated_cost > 0:
            balance = check_x402_balance()
            if balance < estimated_cost:
                emit({
                    'type': 'insufficient_funds',
                    'balance': round(balance, 4),
                    'estimated_cost': round(estimated_cost, 2),
                    'message': f"get ur money up — you don't have enough USDC in your Base wallet. Balance: ${balance:.2f}, need ~${estimated_cost:.2f}",
                })
                # Pause and wait — user can hit "Continue Anyway" from the UI
                _checkpoint(wait_if_paused)

        emit({'type': 'enrich_start', 'count': len(inserted_ids)})
        inserted_lead_map = {lead['id']: lead for lead in inserted_leads}

        total_cost += _run_stage_concurrently(
            inserted_ids,
            lambda lead_id: enrich_lead(lead_id, emit=emit, wait_if_paused=wait_if_paused),
            lambda lead_id, result, completed, total: emit({
                'type': 'enrich_lead',
                'index': completed,
                'total': total,
                'lead_id': lead_id,
                'company': (inserted_lead_map.get(lead_id) or {}).get('company', ''),
                'sources': list(result['sources'].keys()),
            }),
            lambda lead_id, error, completed, total: emit({
                'type': 'enrich_error',
                'message': str(error),
                'lead_id': lead_id,
                'index': completed,
                'total': total,
            }),
            ENRICH_CONCURRENCY,
            wait_if_paused=wait_if_paused,
        )
        emit({'type': 'enrich_done', 'count': len(inserted_ids)})

        inserted_ids = _retier_after_enrichment(inserted_ids, emit=emit, wait_if_paused=wait_if_paused)
        inserted_leads = get_leads_by_ids(inserted_ids)
        inserted_lead_map = {lead['id']: lead for lead in inserted_leads}

        # ── Export stage ───────────────────────────────────────────────────
        try:
            export_leads_to_notion(inserted_ids, emit=emit)
        except Exception as e:
            emit({'type': 'export_error', 'error': str(e), 'message': str(e)})

        # Store cost on the run
        if run_id is not None:
            update_run_cost(run_id, total_cost)

        total = count_leads(conn)
        emit({
            'type': 'done',
            'inserted': inserted_total,
            'skipped_geo': skipped_geo,
            'skipped_dupe': skipped_dupe,
            'total_leads': total,
            'cost': total_cost,
        })

    except Exception as e:
        emit({'type': 'error', 'message': str(e)})
        raise
    finally:
        conn.close()


def run_csv_pipeline(lead_ids: list[int], emit=None, run_id=None, wait_if_paused=None):
    """Run enrichment + email generation on pre-inserted CSV leads (skip search)."""
    if emit is None:
        emit = lambda e: None
    reset_x402_flag()

    total_cost = 0.0

    emit({'type': 'start', 'industries': ['csv_import'], 'cities': [],
          'total_queries': 0})

    inserted_leads = get_leads_by_ids(lead_ids)
    lead_map = {lead['id']: lead for lead in inserted_leads}

    enrichable_fields = [
        'google_maps_url', 'owner_email', 'owner_phone', 'owner_linkedin',
        'employee_count', 'key_staff', 'year_established', 'services_offered',
        'company_description', 'revenue_estimate', 'certifications',
        'review_summary', 'facebook_url', 'yelp_url',
    ]

    for lead in inserted_leads:
        meta = lead.get('enrichment_meta') or {}
        field_values = {}
        field_sources = {}
        for field in enrichable_fields:
            value = lead.get(field)
            if value is None or value == '' or value == []:
                continue
            if isinstance(value, list):
                field_values[field] = ', '.join(str(v) for v in value)
            else:
                field_values[field] = str(value)
            field_sources[field] = (meta.get(field) or {}).get('source') or 'csv_import'

        emit({
            'type': 'insert',
            'lead_id': lead.get('id'),
            'company': lead.get('company', ''),
            'industry': lead.get('industry', ''),
            'city': lead.get('city', ''),
            'distance_miles': lead.get('distance_miles'),
            'ownership_type': lead.get('ownership_type'),
            'field_values': field_values,
            'field_sources': field_sources,
            'generated_subject': lead.get('generated_subject'),
            'generated_email': lead.get('generated_email'),
        })

    try:
        tier_result = tier_leads(lead_ids, emit=emit, wait_if_paused=wait_if_paused)
        tiered_leads = get_leads_by_ids(tier_result['kept_ids'])
        # Enrich all tiers, not just tier_1
        # lead_ids = _emit_non_tier1_skips(tiered_leads, emit)
        lead_ids = [lead['id'] for lead in tiered_leads]
        emit({'type': 'csv_imported', 'count': len(lead_ids)})
        inserted_leads = tiered_leads
        lead_map = {lead['id']: lead for lead in inserted_leads}

        # ── Enrich stage ───────────────────────────────────────────────────
        # ── Pre-flight balance check ──────────────────────────────────────
        estimated_cost = len(lead_ids) * 0.003
        if estimated_cost > 0:
            balance = check_x402_balance()
            if balance < estimated_cost:
                emit({
                    'type': 'insufficient_funds',
                    'balance': round(balance, 4),
                    'estimated_cost': round(estimated_cost, 2),
                    'message': f"get ur money up — you don't have enough USDC in your Base wallet. Balance: ${balance:.2f}, need ~${estimated_cost:.2f}",
                })
                # Pause and wait — user can hit "Continue Anyway" from the UI
                _checkpoint(wait_if_paused)

        emit({'type': 'enrich_start', 'count': len(lead_ids)})
        total_cost += _run_stage_concurrently(
            lead_ids,
            lambda lead_id: enrich_lead(lead_id, emit=emit, wait_if_paused=wait_if_paused),
            lambda lead_id, result, completed, total: emit({
                'type': 'enrich_lead',
                'index': completed,
                'total': total,
                'lead_id': lead_id,
                'company': (lead_map.get(lead_id) or {}).get('company', ''),
                'sources': list(result['sources'].keys()),
            }),
            lambda lead_id, error, completed, total: emit({
                'type': 'enrich_error',
                'message': str(error),
                'lead_id': lead_id,
                'index': completed,
                'total': total,
            }),
            ENRICH_CONCURRENCY,
            wait_if_paused=wait_if_paused,
        )
        emit({'type': 'enrich_done', 'count': len(lead_ids)})

        lead_ids = _retier_after_enrichment(lead_ids, emit=emit, wait_if_paused=wait_if_paused)
        inserted_leads = get_leads_by_ids(lead_ids)
        lead_map = {lead['id']: lead for lead in inserted_leads}

        # ── Export stage ───────────────────────────────────────────────────
        try:
            export_leads_to_notion(lead_ids, emit=emit)
        except Exception as e:
            emit({'type': 'export_error', 'error': str(e), 'message': str(e)})

        # Store cost on the run
        if run_id is not None:
            update_run_cost(run_id, total_cost)

        conn = get_connection()
        total = count_leads(conn)
        conn.close()
        emit({
            'type': 'done',
            'inserted': len(lead_ids),
            'skipped_geo': 0,
            'skipped_dupe': 0,
            'total_leads': total,
            'cost': total_cost,
        })

    except Exception as e:
        emit({'type': 'error', 'message': str(e)})
        raise


if __name__ == '__main__':
    run_pipeline(emit=lambda e: print(e))
