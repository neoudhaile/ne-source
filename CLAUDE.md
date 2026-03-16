# BH Pipeline — Broeren Haile Holdings

## What this project is
Automated SMB lead sourcing pipeline for acquisition targeting.
Searches Google Maps for niche service businesses in Southern California
via the Openmart API (through Orthogonal/x402), geo-filters to the LA
metro, normalizes records, and inserts them into a Postgres database.

## MVP scope — build only this
- One data source: Openmart /api/v1/search
- One table: smb_leads
- One geo target: 40-mile radius from LA center (34.0522, -118.2437)
- No outreach, no enrichment, no email sending in MVP

## File structure
config.py     -- search targets, geo constants, city fallback list
db.py         -- Postgres connection, insert_lead(), count_leads()
scraper.py    -- Openmart API calls via x402
normalize.py  -- geo filter (3-layer), field mapping, validation
pipeline.py   -- main entrypoint, orchestrates the full flow

test_db.py       -- tests DB connection
test_scraper.py  -- tests a single Openmart call
test_normalize.py -- tests geo filter with real results

## Credentials (all from .env via python-dotenv)
PRIVATE_KEY   -- Orthogonal/x402 wallet private key
DB_HOST       -- Postgres host (localhost on VPS)
DB_PORT       -- 5432
DB_NAME       -- broeren_haile
DB_USER       -- bh_admin
DB_PASSWORD   -- Postgres password

## x402 payment pattern — use this exactly, do not deviate
import os, requests
from eth_account import Account
from x402.clients.requests import x402_http_adapter
from dotenv import load_dotenv
load_dotenv()

account = Account.from_key(os.getenv('PRIVATE_KEY'))
session = requests.Session()
session.mount('https://', x402_http_adapter(account))

## Openmart endpoint
POST https://x402.orth.sh/openmart/api/v1/search
Body: { query, page_size, min_rating, min_reviews }
DO NOT pass a geo parameter -- live testing confirmed it has no effect.
Geo filtering is done in normalize.py using haversine distance.

## smb_leads table schema
id, company, owner_name, email, phone, address, city, state, zipcode,
website, industry, google_place_id (UNIQUE), rating, review_count,
ownership_type, distance_miles, latitude, longitude, openmart_id,
status (default 'new'), source (default 'openmart'), raw_data (JSONB),
created_at

## Dedup key
google_place_id extracted from Openmart's source_id field.
source_id format: "GOOGLE_MAP@ChIJ..." -- strip the "GOOGLE_MAP@" prefix.
Insert with: ON CONFLICT (google_place_id) DO NOTHING

## Geo filter in normalize.py -- 3 layers in this order
1. State check: reject if state not in ('California', 'CA')
2. Haversine distance: reject if > 40 miles from (34.0522, -118.2437)
3. City name fallback: check against TARGET_CITIES set in config.py
Return None from normalize_lead() if any layer rejects the record.

## Email validation
Filter out filler domains: godaddy.com, wix.com, squarespace.com,
wordpress.com, example.com -- these appear in Openmart data but are
not real business emails.

## Owner extraction
Check staffs[] array in Openmart response for role containing:
owner, founder, president, ceo -- return that person's name.

## Key decisions already made
- Do NOT use n8n or Clay -- Python scripts replace both
- Openmart geo param does not work as a hard filter (tested live)
- ownership_type field stored as-is: FAMILY, INDEPENDENT, or None
- distance_miles stored for every inserted record
- raw_data JSONB stores the full Openmart response for auditing
- psycopg2 parameterized queries only -- never f-string SQL

## Python packages installed
x402, eth-account, requests, psycopg2-binary, python-dotenv
