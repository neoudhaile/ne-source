"""
AI email generation — fixed template + Claude-generated specific observation.

The email body is a fixed template written by Rigel. Claude's only job is to
write one short "specific observation" sentence about the company, drawn from
enriched data. The vertical thesis comes from vertical_theses.py (no AI needed).

Uses the Anthropic SDK directly (not x402/Orthogonal).
"""

import os
import anthropic
from dotenv import load_dotenv

from pipeline.db import get_lead, update_lead
from pipeline.vertical_theses import get_thesis

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

    owner_first = (lead.get('owner_name') or 'there').split()[0]
    company = lead.get('company') or 'your company'
    vertical = lead.get('industry') or 'service'
    thesis = get_thesis(vertical)

    # If no thesis match, ask Claude to generate the thesis too
    prompt = _build_prompt(lead, thesis_provided=thesis is not None)

    response = client.messages.create(
        model='claude-haiku-4-5-20251001',
        max_tokens=250,
        messages=[{'role': 'user', 'content': prompt}],
    )

    text = response.content[0].text
    fields = _parse_response(text)

    # Use stored thesis or Claude-generated one
    final_thesis = thesis or fields.get('vertical_thesis', '')

    subject = f"{company} \u2014 Potential Partnership Conversation"

    body = (
        f"Hi {owner_first},\n\n"
        f"I came across {company} while researching {vertical} "
        f"in Southern California.\n\n"
        f"My name is Rigel Broeren. I run Broeren & Co. Holdings, a "
        f"Southern California holding company focused on acquiring and "
        f"operating {vertical} businesses for the long term. We're not a "
        f"private equity fund, there's no outside investor with a five-year "
        f"exit clock pushing decisions. We're specifically focused on "
        f"{vertical} because {final_thesis}\n\n"
        f"What we're looking for is straightforward: businesses where the "
        f"owner has built something worth preserving. When we acquire a "
        f"company, the team stays, the name stays, and the owner has a "
        f"real say in how the transition happens \u2014 including staying "
        f"involved if that's what they want.\n\n"
        f"What stood out to me about {company} specifically: "
        f"{fields.get('specific_observation', '')}\n\n"
        f"I'd love to hear the story behind what you've built \u2014 would you "
        f"be open to a 20 minute call?"
    )

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


def _build_prompt(lead: dict, thesis_provided: bool = True) -> str:
    """
    Build the prompt for Claude. If we have a matching vertical thesis,
    Claude only needs to write the specific_observation. If not, Claude
    also writes a vertical_thesis.
    """
    data_block = f"""COMPANY DATA:
- Company name: {_fmt(lead.get('company'))}
- Industry: {_fmt(lead.get('industry'))}
- City: {_fmt(lead.get('city'))}, {_fmt(lead.get('state'))}
- Website: {_fmt(lead.get('website'))}
- Google rating: {_fmt(lead.get('rating'))} ({_fmt(lead.get('review_count'))} reviews)
- Ownership type: {_fmt(lead.get('ownership_type'))}
- Year established: {_fmt(lead.get('year_established'))}
- Services offered: {_fmt(lead.get('services_offered'))}
- Company description: {_fmt(lead.get('company_description'))}
- Certifications: {_fmt(lead.get('certifications'))}
- Employee count: {_fmt(lead.get('employee_count'))}
- Review highlights: {_fmt(lead.get('review_summary'))}
- Revenue estimate: {_fmt(lead.get('revenue_estimate'))}"""

    if thesis_provided:
        return f"""{data_block}

TASK:
Write one sentence (2-3 lines max) explaining what specifically stood out \
about this company. Reference concrete details from the data above — a \
high rating, years in business, specific services, certifications, customer \
sentiment, or anything else that shows genuine familiarity. Do NOT use \
generic praise. Every detail must come from the data provided.

Output format (just the observation, no label):
SPECIFIC_OBSERVATION: <your sentence>"""
    else:
        return f"""{data_block}

TASK:
Write two things:

1. VERTICAL_THESIS — one paragraph (3-4 sentences) explaining why an \
acquisition firm focused on long-term ownership would be interested in \
the {_fmt(lead.get('industry'))} vertical specifically. Talk about the \
demand dynamics, customer loyalty, and market position of established \
independents in this space. Conversational tone, no jargon.

2. SPECIFIC_OBSERVATION — one sentence (2-3 lines max) explaining what \
specifically stood out about this company. Reference concrete details from \
the data above. Do NOT use generic praise.

Output format:
VERTICAL_THESIS: <your paragraph>
SPECIFIC_OBSERVATION: <your sentence>"""


def _parse_response(text: str) -> dict:
    """Extract labeled fields from Claude's response."""
    fields = {}
    current_key = None

    for line in text.strip().split('\n'):
        upper = line.strip().upper()
        if upper.startswith('SPECIFIC_OBSERVATION:'):
            current_key = 'specific_observation'
            fields[current_key] = line.split(':', 1)[1].strip()
        elif upper.startswith('VERTICAL_THESIS:'):
            current_key = 'vertical_thesis'
            fields[current_key] = line.split(':', 1)[1].strip()
        elif current_key:
            fields[current_key] += '\n' + line

    # Clean up
    for key in fields:
        fields[key] = fields[key].strip()

    return fields
