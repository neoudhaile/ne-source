"""
CSV import — upload arbitrary CSVs, use Claude to map columns to smb_leads
schema, insert rows, then hand off to enrichment + email generation.
"""

import csv
import io
import json
import os
import re
import anthropic
from dotenv import load_dotenv

from pipeline.db import get_connection, insert_leads_csv_batch, prepare_csv_lead

load_dotenv()

claude = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

# The DB columns we can accept from CSV data.
# Enrichment columns (owner_email, services_offered, etc.) are intentionally
# included — if the CSV already has them, we should use them and let the
# waterfall skip those fields.
MAPPABLE_COLUMNS = [
    'company', 'owner_name', 'company_email', 'company_phone', 'address', 'city', 'state',
    'zipcode', 'website', 'industry', 'rating', 'review_count',
    'ownership_type', 'latitude', 'longitude',
    'owner_email', 'owner_phone', 'owner_linkedin',
    'employee_count', 'year_established', 'services_offered',
    'company_description', 'revenue_estimate', 'certifications',
    'facebook_url', 'yelp_url',
]

# Type hints for Claude so it can coerce values
COLUMN_TYPES = {
    'rating': 'float',
    'review_count': 'int',
    'latitude': 'float',
    'longitude': 'float',
    'employee_count': 'int',
    'year_established': 'int',
    'services_offered': 'list of strings (comma-separated in CSV)',
    'certifications': 'list of strings (comma-separated in CSV)',
}

HEADER_ALIASES = {
    'name': 'company',
    'company': 'company',
    'company name': 'company',
    'business name': 'company',
    'industry': 'industry',
    'vertical': 'industry',
    'category': 'industry',
    'address': 'address',
    'location': 'address',
    'employees': 'employee_count',
    'employee count': 'employee_count',
    'headcount': 'employee_count',
    'year founded': 'year_established',
    'founded': 'year_established',
    'year established': 'year_established',
    'revenue': 'revenue_estimate',
    'annual revenue': 'revenue_estimate',
}


def _normalize_header(header: str) -> str:
    return ' '.join(str(header or '').strip().lower().replace('_', ' ').split())


def _looks_like_person_linkedin_url(value: str | None) -> bool:
    value = str(value or '').strip().lower()
    return value.startswith('http') and 'linkedin.com/in/' in value


def _looks_like_company_linkedin_url(value: str | None) -> bool:
    value = str(value or '').strip().lower()
    return value.startswith('http') and 'linkedin.com/company/' in value


def _infer_special_header_mapping(header: str, sample_rows: list[dict]) -> str | None:
    normalized = _normalize_header(header)
    if normalized not in {'linkedin', 'linkedin url', 'linkedin profile', 'linkedin profile url'}:
        return None

    values = [
        str(row.get(header) or '').strip()
        for row in sample_rows
        if str(row.get(header) or '').strip()
    ]
    if not values:
        return None
    if any(_looks_like_company_linkedin_url(value) for value in values):
        return None
    if all(_looks_like_person_linkedin_url(value) for value in values):
        return 'owner_linkedin'
    return None


def map_columns(csv_headers: list[str], sample_rows: list[dict]) -> dict:
    """
    Use Claude to map CSV column names to smb_leads schema columns.
    Returns a dict: {csv_column_name: db_column_name} for matched columns.
    Unmatched CSV columns are excluded.
    """
    direct_mapping = {}
    unresolved_headers = []
    for header in csv_headers:
        normalized = _normalize_header(header)
        if normalized in {'linkedin', 'linkedin url', 'linkedin profile', 'linkedin profile url'}:
            db_col = _infer_special_header_mapping(header, sample_rows)
            if db_col:
                direct_mapping[header] = db_col
            continue

        db_col = HEADER_ALIASES.get(normalized)
        if db_col:
            direct_mapping[header] = db_col
        else:
            unresolved_headers.append(header)

    if not unresolved_headers:
        return direct_mapping

    prompt = f"""I have a CSV with these column headers:
{json.dumps(unresolved_headers)}

Here are 3 sample rows:
{json.dumps(sample_rows[:3], indent=2)}

I need to map these CSV columns to my database schema. Here are the valid database columns:
{json.dumps(MAPPABLE_COLUMNS)}

Rules:
- Map each CSV column to the most appropriate database column, or null if no match.
- "company" = business name / company name / dba name
- "owner_name" = owner / proprietor / contact name / principal
- "company_email" = general business email (info@, contact@, sales@)
- "owner_email" = owner's personal/professional email
- "company_phone" = business phone
- "owner_phone" = owner's personal phone
- "owner_linkedin" = a person's LinkedIn profile URL (usually `/in/`), never a company LinkedIn page (`/company/`)
- "address" = street address (not city/state/zip — those are separate columns)
- "industry" = business type / category / vertical / trade
- "website" = company website URL
- If the CSV has a combined "city, state" or "city, state zip" column, map it to "city" — I'll parse it.
- If the CSV has a combined "full address" column with street+city+state+zip, map it to "address" — I'll parse it.

Return ONLY a JSON object mapping CSV column names to database column names (or null).
Example: {{"Business Name": "company", "Owner": "owner_name", "Notes": null}}"""

    response = claude.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=500,
        messages=[{'role': 'user', 'content': prompt}],
    )

    text = response.content[0].text
    if '```' in text:
        text = text.split('```')[1]
        if text.startswith('json'):
            text = text[4:]
        text = text.strip()

    raw_mapping = json.loads(text)
    # Filter out nulls and invalid targets
    inferred_mapping = {
        csv_col: db_col
        for csv_col, db_col in raw_mapping.items()
        if db_col and db_col in MAPPABLE_COLUMNS
    }
    return direct_mapping | inferred_mapping


def _coerce_value(db_col: str, raw_value: str):
    """Convert a string CSV value to the appropriate Python type."""
    if raw_value is None or str(raw_value).strip() == '':
        return None

    raw_value = str(raw_value).strip()
    col_type = COLUMN_TYPES.get(db_col)

    if col_type == 'int':
        import re
        match = re.match(r'^\s*(\d[\d,]*)\s*[-to]+\s*(\d[\d,]*)\s*$', raw_value, re.IGNORECASE)
        if match:
            low = int(match.group(1).replace(',', ''))
            high = int(match.group(2).replace(',', ''))
            return int(round((low + high) / 2))
        plus_match = re.match(r'^\s*(\d[\d,]*)\s*\+\s*$', raw_value)
        if plus_match:
            return int(plus_match.group(1).replace(',', ''))
        try:
            return int(float(raw_value))
        except (ValueError, TypeError):
            return None
    elif col_type == 'float':
        try:
            return float(raw_value)
        except (ValueError, TypeError):
            return None
    elif col_type and 'list' in col_type:
        # Split comma-separated values into a list
        return [v.strip() for v in raw_value.split(',') if v.strip()]

    return raw_value


def _sanitize_mapped_value(db_col: str, raw_value):
    if db_col == 'owner_linkedin' and _looks_like_company_linkedin_url(raw_value):
        return None
    return raw_value


def _parse_city_state_zip(value: str) -> dict:
    """Try to parse 'City, ST 12345' or 'City, State' into components."""
    parts = {}
    if not value:
        return parts

    # Try "City, ST 12345" pattern
    import re
    match = re.match(r'^(.+?),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?$', value.strip())
    if match:
        parts['city'] = match.group(1).strip()
        parts['state'] = match.group(2).strip()
        if match.group(3):
            parts['zipcode'] = match.group(3).strip()
        return parts

    # Try "City, State" with full state name
    match = re.match(r'^(.+?),\s*(.+)$', value.strip())
    if match:
        parts['city'] = match.group(1).strip()
        state_part = match.group(2).strip()
        # Check for trailing zip
        zip_match = re.search(r'\s+(\d{5}(?:-\d{4})?)$', state_part)
        if zip_match:
            parts['zipcode'] = zip_match.group(1)
            state_part = state_part[:zip_match.start()].strip()
        parts['state'] = state_part
        return parts

    return parts


def _parse_full_address(value: str) -> dict:
    """
    Try to parse a full address like:
    "3900 Via Oro Avenue, Long Beach, CA 90810, United States"
    """
    if not value:
        return {}

    parts = [part.strip() for part in value.split(',') if part.strip()]
    if len(parts) < 3:
        return {'address': value.strip()}

    parsed = {'address': parts[0]}
    parsed.update(_parse_city_state_zip(', '.join(parts[1:3])))
    return parsed


def import_csv(file_content: str | bytes, emit=None) -> dict:
    """
    Import leads from CSV content.
    Returns {'inserted': int, 'skipped': int, 'total': int, 'mapping': dict, 'lead_ids': list}.
    """
    if emit is None:
        emit = lambda e: None

    if isinstance(file_content, bytes):
        file_content = file_content.decode('utf-8-sig')  # handle BOM

    reader = csv.DictReader(io.StringIO(file_content))
    headers = reader.fieldnames or []
    rows = list(reader)

    if not headers or not rows:
        raise ValueError('CSV is empty or has no headers')

    emit({'type': 'csv_parse', 'headers': headers, 'row_count': len(rows)})

    # Step 1: Map columns via Claude
    sample_rows = rows[:3]
    mapping = map_columns(headers, sample_rows)

    emit({'type': 'csv_mapping', 'mapping': mapping})

    # Step 2: Prepare each row, then insert in batches to avoid thousands of
    # remote commit round-trips to Postgres.
    conn = get_connection()
    inserted = 0
    skipped = 0
    lead_ids = []
    pending_rows = []

    try:
        for i, row in enumerate(rows):
            lead_dict = {
                'raw_data': json.dumps(row),
            }

            for csv_col, db_col in mapping.items():
                raw = row.get(csv_col)
                if raw is None or str(raw).strip() == '':
                    continue

                # If Claude mapped a combined city/state field to "city", parse it
                if db_col == 'city' and ',' in str(raw):
                    parsed = _parse_city_state_zip(str(raw))
                    for k, v in parsed.items():
                        if k not in lead_dict or not lead_dict[k]:
                            lead_dict[k] = v
                    continue

                if db_col == 'address' and ',' in str(raw):
                    parsed = _parse_full_address(str(raw))
                    for k, v in parsed.items():
                        if k not in lead_dict or not lead_dict[k]:
                            lead_dict[k] = v
                    continue

                coerced = _coerce_value(db_col, raw)
                sanitized = _sanitize_mapped_value(db_col, coerced)
                if sanitized is not None:
                    lead_dict[db_col] = sanitized

            # Skip rows with no company name
            if not lead_dict.get('company'):
                skipped += 1
                emit({'type': 'csv_skip', 'row': i + 1, 'reason': 'no company name'})
                continue

            pending_rows.append({
                'row_num': i + 1,
                'company': lead_dict.get('company', ''),
                'lead_dict': prepare_csv_lead(lead_dict),
            })

        batch_size = 250
        for start in range(0, len(pending_rows), batch_size):
            batch = pending_rows[start:start + batch_size]
            inserted_map = insert_leads_csv_batch(
                conn,
                [item['lead_dict'] for item in batch],
            )

            for item in batch:
                lead_id = inserted_map.get(item['lead_dict']['google_place_id'])
                if lead_id is not None:
                    inserted += 1
                    lead_ids.append(lead_id)
                    emit({
                        'type': 'csv_insert',
                        'lead_id': lead_id,
                        'row': item['row_num'],
                        'company': item['company'],
                    })
                else:
                    skipped += 1
                    emit({
                        'type': 'csv_skip',
                        'row': item['row_num'],
                        'reason': 'duplicate',
                        'company': item['company'],
                    })

            emit({
                'type': 'csv_batch_done',
                'processed': min(start + batch_size, len(pending_rows)),
                'total_pending': len(pending_rows),
                'inserted_so_far': inserted,
                'skipped_so_far': skipped,
            })
    finally:
        conn.close()

    emit({
        'type': 'csv_done',
        'inserted': inserted,
        'skipped': skipped,
        'total': len(rows),
    })

    return {
        'inserted': inserted,
        'skipped': skipped,
        'total': len(rows),
        'mapping': mapping,
        'lead_ids': lead_ids,
    }
