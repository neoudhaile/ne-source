"""
AI email generation — Claude produces a unique subject + body per lead.

Uses the Anthropic SDK directly (not x402/Orthogonal).
"""

import os
import anthropic
from dotenv import load_dotenv

from pipeline.db import get_lead, update_lead

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))


def generate_email(lead_id: int) -> dict:
    """
    Generate a personalized email for one lead.
    Returns {'cost': float}.
    """
    lead = get_lead(lead_id)
    if not lead:
        return {'cost': 0.0}

    prompt = _build_prompt(lead)

    response = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=500,
        messages=[{'role': 'user', 'content': prompt}],
    )

    text = response.content[0].text
    subject, body = _parse_response(text)

    update_lead(lead_id, {
        'generated_subject': subject,
        'generated_email': body,
    })

    cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
    return {'cost': cost}


def _fmt(val, fallback='Not available'):
    """Format a value for the prompt — handle None, empty, lists."""
    if val is None or val == '' or val == []:
        return fallback
    if isinstance(val, list):
        return ', '.join(str(v) for v in val)
    return str(val)


def _build_prompt(lead: dict) -> str:
    return f"""COMPANY DATA:
- Company name: {_fmt(lead.get('company'))}
- Industry: {_fmt(lead.get('industry'))}
- Address: {_fmt(lead.get('address'))}
- City: {_fmt(lead.get('city'))}, {_fmt(lead.get('state'))} {_fmt(lead.get('zipcode'), '')}
- Website: {_fmt(lead.get('website'))}
- Google Maps: {_fmt(lead.get('google_maps_url'))}
- Google rating: {_fmt(lead.get('rating'))} ({_fmt(lead.get('review_count'))} reviews)
- Ownership type: {_fmt(lead.get('ownership_type'))}
- Distance from LA: {_fmt(lead.get('distance_miles'))} miles

OWNER / CONTACT:
- Owner name: {_fmt(lead.get('owner_name'))}
- Owner title: {_fmt(lead.get('owner_title'))}
- Owner email: {_fmt(lead.get('owner_email'))}
- Owner phone: {_fmt(lead.get('owner_phone'))}
- Owner LinkedIn: {_fmt(lead.get('owner_linkedin'))}

TEAM:
- Employee count: {_fmt(lead.get('employee_count'))}
- Key staff: {_fmt(lead.get('key_staff'))}

BUSINESS DETAIL:
- Year established: {_fmt(lead.get('year_established'))}
- Services offered: {_fmt(lead.get('services_offered'))}
- Company description: {_fmt(lead.get('company_description'))}
- Revenue estimate: {_fmt(lead.get('revenue_estimate'))}
- Certifications: {_fmt(lead.get('certifications'))}

REPUTATION:
- Review highlights: {_fmt(lead.get('review_summary'))}
- Facebook: {_fmt(lead.get('facebook_url'))}
- Yelp: {_fmt(lead.get('yelp_url'))}

CONTEXT:
Broeren Haile Holdings is an acquisition firm looking to acquire \
established service businesses in the LA metro area. We want to \
reach out to {_fmt(lead.get('owner_name'), 'the owner')} to explore whether they'd be open to \
a conversation about a potential acquisition or partnership.

INSTRUCTIONS:
Write a short, warm, personalized email (3-5 sentences max) from \
our team to {_fmt(lead.get('owner_name'), 'the owner')}. Reference specific details about their \
business that show we've done our research. Keep the tone \
conversational — not salesy. End with a soft ask for a brief call. \
Do not use generic filler. Every sentence should be specific to \
this company. Use only data provided above — do not invent facts.

Output format:
SUBJECT: <subject line>
BODY: <email body>"""


def _parse_response(text: str) -> tuple[str, str]:
    """Extract SUBJECT: and BODY: from Claude's response."""
    subject = ''
    body = ''

    lines = text.strip().split('\n')
    in_body = False

    for line in lines:
        if line.strip().upper().startswith('SUBJECT:'):
            subject = line.split(':', 1)[1].strip()
        elif line.strip().upper().startswith('BODY:'):
            body = line.split(':', 1)[1].strip()
            in_body = True
        elif in_body:
            body += '\n' + line

    return subject.strip(), body.strip()
