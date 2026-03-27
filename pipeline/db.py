import os
import psycopg2
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


def insert_lead(conn, lead_dict):
    sql = """
        INSERT INTO smb_leads (
            company, owner_name, email, phone, address, city, state,
            zipcode, website, industry, google_place_id, rating, review_count,
            ownership_type, distance_miles, latitude, longitude, openmart_id,
            status, source, raw_data
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (google_place_id) DO NOTHING
    """
    values = (
        lead_dict.get('company'),
        lead_dict.get('owner_name'),
        lead_dict.get('email'),
        lead_dict.get('phone'),
        lead_dict.get('address'),
        lead_dict.get('city'),
        lead_dict.get('state'),
        lead_dict.get('zipcode'),
        lead_dict.get('website'),
        lead_dict.get('industry'),
        lead_dict.get('google_place_id'),
        lead_dict.get('rating'),
        lead_dict.get('review_count'),
        lead_dict.get('ownership_type'),
        lead_dict.get('distance_miles'),
        lead_dict.get('latitude'),
        lead_dict.get('longitude'),
        lead_dict.get('openmart_id'),
        lead_dict.get('status'),
        lead_dict.get('source'),
        lead_dict.get('raw_data'),
    )
    cur = conn.cursor()
    try:
        cur.execute(sql + ' RETURNING id', values)
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None
    finally:
        cur.close()


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
