---
name: hunter
description: Call Hunter API via Orthogonal. Available endpoints: GET /v2/combined/find, GET /v2/people/find, GET /v2/email-count, POST /v2/discover, GET /v2/companies/find, GET /v2/domain-search, GET /v2/email-finder, GET /v2/email-verifier. Use when working with Hunter or when user asks to Hunter.io API for finding and verifying professional email addresses. Domain search, email finder, email verification, and company/person enrichment.. Requires ORTHOGONAL_API_KEY.
---

# Hunter

Hunter.io API for finding and verifying professional email addresses. Domain search, email finder, email verification, and company/person enrichment.

When the user asks to use Hunter or any of its endpoints, use the code patterns below. The user's request is: $ARGUMENTS

## Available Endpoints

### GET /v2/combined/find
Get both person AND company information from an email address in a single request.
- **Price**: $0.01
- **Query params**:
  - `email` (string, required): Email address to enrich


### GET /v2/people/find
Get detailed person information from an email address - name, location, employment, social profiles.
- **Price**: $0.01
- **Query params**:
  - `email` (string): Email address to enrich
  - `linkedin_handle` (string): LinkedIn handle to enrich


### GET /v2/email-count
Get count of email addresses we have for a domain, broken down by department and seniority. FREE endpoint.
- **Price**: $0.01
- **Query params**:
  - `domain` (string): Domain to count emails for
  - `company` (string): Company name (domain preferred)
  - `type` (string): Filter: personal or generic


### POST /v2/discover
Find companies matching criteria using filters or natural language. Returns up to 100 companies per request. FREE endpoint.
- **Price**: $0.01

- **Body params**:
  - `query` (string): Natural language search (e.g. Companies in Europe in Tech)
  - `headquarters_location` (object): Filter by HQ location
  - `industry` (object): Filter by industry
  - `headcount` (array): Filter by employee count ranges
  - `limit` (integer): Max results (default 100)
  - `offset` (integer): Skip N results for pagination

### GET /v2/companies/find
Get detailed company information from a domain - industry, description, location, size, tech stack, funding.
- **Price**: $0.01
- **Query params**:
  - `domain` (string, required): Company domain to enrich (e.g. hunter.io)


### GET /v2/domain-search
Find all email addresses for a domain. Returns emails with sources, confidence scores, and verification status.
- **Price**: $0.01
- **Query params**:
  - `domain` (string, required): Domain to search (e.g. stripe.com)
  - `limit` (integer): Max emails to return (default 10)
  - `offset` (integer): Skip N emails
  - `type` (string): Filter: personal or generic
  - `seniority` (string): Filter: junior, senior, or executive
  - `department` (string): Filter by department (sales, marketing, etc)


### GET /v2/email-finder
Find the most likely email address for a person given their name and company domain.
- **Price**: $0.01
- **Query params**:
  - `domain` (string): Company domain (e.g. reddit.com)
  - `company` (string): Company name (domain preferred)
  - `first_name` (string): Person first name
  - `last_name` (string): Person last name
  - `full_name` (string): Full name (if first/last not available)
  - `linkedin_handle` (string): LinkedIn profile handle


### GET /v2/email-verifier
Verify if an email address is deliverable. Returns status (valid, invalid, accept_all, webmail, disposable, unknown).
- **Price**: $0.01
- **Query params**:
  - `email` (string, required): Email address to verify


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
  api: "hunter",
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
        'api': 'hunter',
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
  -d '{"api": "hunter", "path": "/endpoint-path"}'
```
