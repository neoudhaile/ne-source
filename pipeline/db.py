import hashlib
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
    )


LEAD_COLUMNS = [
    'company', 'owner_name', 'email', 'phone', 'address', 'city', 'state',
    'zipcode', 'website', 'industry', 'google_place_id', 'rating',
    'review_count', 'ownership_type', 'distance_miles', 'latitude',
    'longitude', 'openmart_id', 'run_id',
    'owner_email', 'owner_phone', 'owner_linkedin',
    'employee_count', 'key_staff', 'year_established', 'services_offered',
    'company_description', 'revenue_estimate', 'certifications',
    'review_summary', 'facebook_url', 'yelp_url', 'google_maps_url',
    'enrichment_meta',
    'status', 'source', 'raw_data',
]

INSERT_LEAD_SQL = """
    INSERT INTO smb_leads (
        company, owner_name, email, phone, address, city, state,
        zipcode, website, industry, google_place_id, rating, review_count,
        ownership_type, distance_miles, latitude, longitude, openmart_id, run_id,
        owner_email, owner_phone, owner_linkedin,
        employee_count, key_staff, year_established, services_offered,
        company_description, revenue_estimate, certifications,
        review_summary, facebook_url, yelp_url, google_maps_url,
        enrichment_meta,
        status, source, raw_data
    ) VALUES %s
    ON CONFLICT (google_place_id) DO NOTHING
"""


def _lead_values(lead_dict):
    return tuple(lead_dict.get(col) for col in LEAD_COLUMNS)


def ensure_run_column(conn):
    cur = conn.cursor()
    try:
        cur.execute('ALTER TABLE smb_leads ADD COLUMN IF NOT EXISTS run_id INTEGER')
        conn.commit()
    finally:
        cur.close()


def insert_lead(conn, lead_dict):
    values = _lead_values(lead_dict)
    cur = conn.cursor()
    try:
        ensure_run_column(conn)
        placeholders = '(' + ', '.join(['%s'] * len(LEAD_COLUMNS)) + ')'
        cur.execute(INSERT_LEAD_SQL.replace('VALUES %s', f'VALUES {placeholders}') + ' RETURNING id', values)
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None
    finally:
        cur.close()


def prepare_csv_lead(lead_dict):
    """
    Add CSV-specific defaults and a synthetic dedup key without mutating
    the caller's original dict.
    """
    prepared = dict(lead_dict)
    dedup_input = (
        (prepared.get('company') or '') +
        (prepared.get('address') or '') +
        (prepared.get('city') or '')
    ).lower().strip()
    if not dedup_input:
        dedup_input = str(id(lead_dict))

    if not prepared.get('google_place_id'):
        prepared['google_place_id'] = 'CSV_' + hashlib.sha256(
            dedup_input.encode()
        ).hexdigest()[:16]
    if not prepared.get('source'):
        prepared['source'] = 'csv_import'
    if not prepared.get('status'):
        prepared['status'] = 'new'
    return prepared


def count_leads(conn):
    cur = conn.cursor()
    try:
        cur.execute('SELECT COUNT(*) FROM smb_leads')
        return cur.fetchone()[0]
    finally:
        cur.close()


def get_lead(lead_id: int) -> dict | None:
    """Fetch a single lead by ID, return as dict."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM smb_leads WHERE id = %s', (lead_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    finally:
        cur.close()
        conn.close()


def get_leads_by_ids(lead_ids: list[int]) -> list[dict]:
    """Fetch multiple leads and preserve the input ID order."""
    if not lead_ids:
        return []

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM smb_leads WHERE id = ANY(%s)', (lead_ids,))
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        row_map = {row[0]: dict(zip(cols, row)) for row in rows}
        return [row_map[lead_id] for lead_id in lead_ids if lead_id in row_map]
    finally:
        cur.close()
        conn.close()


def get_lead_by_google_place_id(google_place_id: str) -> dict | None:
    if not google_place_id:
        return None

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('SELECT * FROM smb_leads WHERE google_place_id = %s LIMIT 1', (google_place_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    finally:
        cur.close()
        conn.close()


def set_leads_run_id(conn, lead_ids: list[int], run_id: int):
    if not lead_ids:
        return
    ensure_run_column(conn)
    cur = conn.cursor()
    try:
        cur.execute('UPDATE smb_leads SET run_id = %s WHERE id = ANY(%s)', (run_id, lead_ids))
        conn.commit()
    finally:
        cur.close()


def get_tier1_leads_for_run(run_id: int) -> list[dict]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        ensure_run_column(conn)
        cur.execute(
            '''
            SELECT company,
                   tier,
                   tier_reason,
                   COALESCE(owner_phone, phone) AS best_phone,
                   COALESCE(owner_email, email) AS best_email,
                   owner_linkedin,
                   website,
                   city,
                   state,
                   industry,
                   employee_count,
                   year_established,
                   revenue_estimate,
                   services_offered,
                   company_description,
                   certifications,
                   review_summary,
                   facebook_url,
                   yelp_url,
                   google_maps_url
            FROM smb_leads
            WHERE run_id = %s AND tier = 'tier_1'
            ORDER BY company ASC
            ''',
            (run_id,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def ensure_tiering_columns(conn):
    """Add tiering columns if the DB has not been migrated yet."""
    cur = conn.cursor()
    try:
        cur.execute('ALTER TABLE smb_leads ADD COLUMN IF NOT EXISTS tier TEXT')
        cur.execute('ALTER TABLE smb_leads ADD COLUMN IF NOT EXISTS tier_reason TEXT')
        conn.commit()
    finally:
        cur.close()


def update_lead(lead_id: int, fields: dict):
    """Update specific columns on an existing lead."""
    if not fields:
        return
    conn = get_connection()
    cur = conn.cursor()
    try:
        set_clause = ', '.join(f'{col} = %s' for col in fields.keys())
        values = list(fields.values())
        values.append(lead_id)
        cur.execute(f'UPDATE smb_leads SET {set_clause} WHERE id = %s', values)
        conn.commit()
    finally:
        cur.close()
        conn.close()


def update_lead_tiers(conn, rows: list[tuple[int, str, str]]):
    """Persist tier and tier_reason for many leads in one query."""
    if not rows:
        return

    cur = conn.cursor()
    try:
        execute_values(
            cur,
            """
            UPDATE smb_leads AS leads
            SET tier = data.tier,
                tier_reason = data.tier_reason
            FROM (VALUES %s) AS data(id, tier, tier_reason)
            WHERE leads.id = data.id
            """,
            rows,
            page_size=len(rows),
        )
        conn.commit()
    finally:
        cur.close()


def delete_leads(conn, lead_ids: list[int]):
    """Delete leads in bulk, typically for hard removes."""
    if not lead_ids:
        return

    cur = conn.cursor()
    try:
        cur.execute('DELETE FROM smb_leads WHERE id = ANY(%s)', (lead_ids,))
        conn.commit()
    finally:
        cur.close()


def insert_lead_csv(conn, lead_dict):
    """
    Insert a lead from CSV import. Uses a hash of company+address as
    google_place_id for dedup since CSV data won't have a Google place ID.
    Returns the new row ID or None if duplicate.
    """
    return insert_lead(conn, prepare_csv_lead(lead_dict))


def insert_leads_csv_batch(conn, lead_dicts):
    """
    Insert many CSV leads in one round-trip. Returns a mapping of
    google_place_id -> inserted row ID for rows that were newly inserted.
    """
    ensure_run_column(conn)
    prepared = [prepare_csv_lead(lead) for lead in lead_dicts]
    if not prepared:
        return {}

    cur = conn.cursor()
    try:
        execute_values(
            cur,
            INSERT_LEAD_SQL + ' RETURNING id, google_place_id',
            [_lead_values(lead) for lead in prepared],
            page_size=len(prepared),
        )
        rows = cur.fetchall()
        conn.commit()
        return {google_place_id: row_id for row_id, google_place_id in rows}
    finally:
        cur.close()


def update_run_cost(run_id: int, cost: float):
    """Store total enrichment + generation cost on the run."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute('UPDATE pipeline_runs SET cost = %s WHERE id = %s', (cost, run_id))
        conn.commit()
    finally:
        cur.close()
        conn.close()
