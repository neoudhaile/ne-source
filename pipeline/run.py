import time
from pipeline.config import INDUSTRIES, CITIES, MIN_REVIEWS, MIN_RATING, MAX_LEADS_PER_RUN
from pipeline.db import get_connection, insert_lead, count_leads, get_lead, update_run_cost
from pipeline.scraper import search_businesses, API_PAGE_SIZE
from pipeline.normalize import normalize_lead
from pipeline.enrichment import enrich_lead
from pipeline.email_generator import generate_email
from pipeline.instantly import push_leads


def run_pipeline(emit=None, run_id=None):
    if emit is None:
        emit = lambda e: None

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
                    lead = normalize_lead(record, industry)

                    if lead is None:
                        skipped_geo += 1
                        batch_rejected += 1
                        continue

                    lead_id = insert_lead(conn, lead)
                    if lead_id is not None:
                        inserted_total += 1
                        batch_inserted += 1
                        inserted_ids.append(lead_id)
                        inserted_leads.append(lead)
                        emit({
                            'type': 'insert',
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

        # ── Enrich stage ───────────────────────────────────────────────────
        total_cost = 0.0

        emit({'type': 'enrich_start', 'count': len(inserted_ids)})
        for i, lead_id in enumerate(inserted_ids):
            try:
                result = enrich_lead(lead_id, emit=emit)
                total_cost += result['cost']
                emit({
                    'type': 'enrich_lead',
                    'index': i + 1,
                    'total': len(inserted_ids),
                    'company': inserted_leads[i].get('company', ''),
                    'sources': list(result['sources'].keys()),
                })
            except Exception as e:
                emit({'type': 'enrich_error', 'message': str(e), 'lead_id': lead_id})
        emit({'type': 'enrich_done', 'count': len(inserted_ids)})

        # ── Generate stage ─────────────────────────────────────────────────
        emit({'type': 'generate_start', 'count': len(inserted_ids)})
        for i, lead_id in enumerate(inserted_ids):
            try:
                result = generate_email(lead_id)
                total_cost += result['cost']
                emit({
                    'type': 'generate_lead',
                    'index': i + 1,
                    'total': len(inserted_ids),
                    'company': inserted_leads[i].get('company', ''),
                })
            except Exception as e:
                emit({'type': 'generate_error', 'message': str(e), 'lead_id': lead_id})
        emit({'type': 'generate_done', 'count': len(inserted_ids)})

        # Store cost on the run
        if run_id is not None:
            update_run_cost(run_id, total_cost)

        # ── Outreach stage ─────────────────────────────────────────────────
        # Re-read leads from DB to get enriched + generated data
        leads_to_push = [get_lead(lid) for lid in inserted_ids]
        leads_to_push = [l for l in leads_to_push if l and (l.get('generated_email') or l.get('email'))]

        emit({'type': 'outreach_start', 'count': len(leads_to_push)})
        try:
            result = push_leads(leads_to_push)
            emit({
                'type':    'outreach_done',
                'pushed':  result['pushed'],
                'skipped': result['skipped'],
                'failed':  result['failed'],
            })
        except Exception as e:
            emit({'type': 'outreach_error', 'message': str(e)})

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


if __name__ == '__main__':
    run_pipeline(emit=lambda e: print(e))
