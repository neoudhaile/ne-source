---
name: sixtyfour
description: Call Sixtyfour API API via Orthogonal. Available endpoints: POST /enrich-company, POST /enrich-lead, POST /find-phone, POST /find-email. Use when working with Sixtyfour API or when user asks to Build custom research agents to enrich people and company data, and surface real-time signals all with a simple API call.. Requires ORTHOGONAL_API_KEY.
---

# Sixtyfour API

Build custom research agents to enrich people and company data, and surface real-time signals all with a simple API call.

When the user asks to use Sixtyfour API or any of its endpoints, use the code patterns below. The user's request is: $ARGUMENTS

## Available Endpoints

### POST /enrich-company
Enrich company data with additional information and find associated people.
- **Price**: $0.1

- **Body params**:
  - `target_company` (object, required): Company data to enrich
  - `struct` (object, required): Fields to collect
  - `lead_struct` (object): Custom schema to define the structure of returned lead data.
  - `find_people` (boolean): Whether to find people associated with the company
  - `research_plan` (string): Optional strategy describing how the agent should search for information
  - `people_focus_prompt` (string): Description of people to find, typically includes the roles or responsibilities of the people you’re looking for

### POST /enrich-lead
Enrich lead information with additional details such as contact information, social profiles, and company details.
- **Price**: $0.1

- **Body params**:
  - `lead_info` (object, required): Initial lead information as key-value pairs
  - `struct` (object, required): Fields to collect about the lead
  - `research_plan` (string): Optional research plan to guide enrichment

### POST /find-phone
The Find Phone API uses Sixtyfour AI to discover phone numbers for leads. It extracts contact information from lead data and returns enriched results with phone numbers.
- **Price**: $0.3

- **Body params**:
  - `lead` (object, required): Lead information object
  - `name` (string): Full name of the person
  - `company` (string): Company name
  - `linkedin_url` (string): LinkedIn profile URL
  - `domain` (string): Company website domain
  - `email` (string): Email address

### POST /find-email
Find email address for a lead.
- **Price**: Dynamic

- **Body params**:
  - `lead` (object, required): Lead information to find email for
  - `mode` (string): Email discovery mode. Allowed values: `"PROFESSIONAL"` (default) for company emails, `"PERSONAL"` for personal emails.

## Integration

Set the environment variable:
```bash
export ORTHOGONAL_API_KEY=orth_live_your_api_key
```

### JavaScript/TypeScript

```javascript
import Orthogonal from "@orth/sdk";

const orthogonal = new Orthogonal({
  apiKey: process.env.ORTHOGONAL_API_KEY,
});

const result = await orthogonal.run({
  api: "sixtyfour",
  path: "/endpoint-path",  // Use the appropriate endpoint from above
  query: { /* query params if needed */ },
  body: { /* body params if needed */ }
});

console.log(result);
```

### Python

```python
import os
import requests

response = requests.post(
    'https://api.orth.sh/v1/run',
    json={
        'api': 'sixtyfour',
        'path': '/endpoint-path',  # Use the appropriate endpoint from above
        'query': {},  # Add query params if needed
        'body': {}    # Add body params if needed
    },
    headers={
        'Authorization': f'Bearer {os.getenv("ORTHOGONAL_API_KEY")}',
        'Content-Type': 'application/json'
    }
)
print(response.json())
```

### cURL

```bash
curl -X POST 'https://api.orth.sh/v1/run' \
  -H 'Authorization: Bearer $ORTHOGONAL_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{"api": "sixtyfour", "path": "/endpoint-path"}'
```
