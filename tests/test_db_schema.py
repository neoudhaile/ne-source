"""Verify the DB schema matches the codebase after the column rename."""
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def _columns():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
    )
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'smb_leads'
        """)
        return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def test_company_email_column_exists():
    cols = _columns()
    assert 'company_email' in cols
    assert 'email' not in cols


def test_company_phone_column_exists():
    cols = _columns()
    assert 'company_phone' in cols
    assert 'phone' not in cols
