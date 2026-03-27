---
name: apollo
description: Call Apollo API API via Orthogonal. Available endpoints: GET /api/v1/organizations/enrich, POST /api/v1/organizations/bulk_enrich, GET /api/v1/organizations/{organization_id}/job_postings, POST /api/v1/news_articles/search, GET /api/v1/organizations/{id}, POST /api/v1/people/match, POST /api/v1/people/bulk_match, POST /api/v1/mixed_companies/search, POST /api/v1/mixed_people/api_search. Use when working with Apollo API or when user asks to Apollo.io API for people and company enrichment, search, and prospecting. Access the Apollo database of 210M+ contacts and 30M+ companies.. Requires ORTHOGONAL_API_KEY.
---

# Apollo API

Apollo.io API for people and company enrichment, search, and prospecting. Access the Apollo database of 210M+ contacts and 30M+ companies.

When the user asks to use Apollo API or any of its endpoints, use the code patterns below. The user's request is: $ARGUMENTS

## Available Endpoints

### GET /api/v1/organizations/enrich
Enrich a company by domain. Returns industry, revenue, employee count, funding, locations, and more.
- **Price**: $0.01
- **Query params**:
  - `domain` (string, required): Company domain to enrich (e.g., apollo.io)


### POST /api/v1/organizations/bulk_enrich
Enrich up to 10 organizations in a single request.
- **Price**: $0.05

- **Body params**:
  - `domains` (array, required): Array of company domains to enrich (max 10)

### GET /api/v1/organizations/{organization_id}/job_postings
Get current job postings for a company by Apollo organization ID.
- **Price**: $0.01
- **Query params**:
  - `organization_id` (string, required): Apollo organization ID (path param)


### POST /api/v1/news_articles/search
Search for news articles related to companies in the Apollo database.
- **Price**: $0.01

- **Body params**:
  - `organization_ids` (array, required): Apollo organization IDs to get news for (required)
  - `q_keywords` (string): Keywords to search in articles
  - `page` (integer): Page number
  - `per_page` (integer): Results per page

### GET /api/v1/organizations/{id}
Get complete organization info by Apollo organization ID.
- **Price**: $0.01
- **Query params**:
  - `id` (string, required): Apollo organization ID (path param)


### POST /api/v1/people/match
Enrich a person by email, LinkedIn URL, name+company, or other identifiers. Returns contact details, job info, and social profiles.
- **Price**: $0.01

- **Body params**:
  - `email` (string): Email address to match
  - `linkedin_url` (string): LinkedIn profile URL
  - `first_name` (string): First name (use with last_name and organization)
  - `last_name` (string): Last name
  - `organization_name` (string): Company name
  - `domain` (string): Company domain
  - `reveal_personal_emails` (boolean): Include personal emails in response
  - `reveal_phone_number` (boolean): Include phone numbers in response

### POST /api/v1/people/bulk_match
Enrich up to 10 people in a single request. Webhook required for async results.
- **Price**: $0.05

- **Body params**:
  - `details` (array, required): Array of person objects to enrich (max 10)
  - `reveal_personal_emails` (boolean): Include personal emails
  - `reveal_phone_number` (boolean): Include phone numbers
  - `webhook_url` (string, required): HTTPS webhook URL for results

### POST /api/v1/mixed_companies/search
Search Apollo database for companies matching filters. Returns up to 100 results per page.
- **Price**: $0.01

- **Body params**:
  - `organization_locations` (array): HQ locations to filter by
  - `organization_num_employees_ranges` (array): Employee count ranges
  - `organization_industry_tag_ids` (array): Industry tag IDs
  - `q_keywords` (string): Keywords to search
  - `page` (integer): Page number (1-500)
  - `per_page` (integer): Results per page (max 100)

### POST /api/v1/mixed_people/api_search
Search Apollo database for people matching filters. Returns up to 100 results per page. Does not include emails/phones - use enrichment endpoints for that.
- **Price**: $0.01

- **Body params**:
  - `person_titles` (array): Job titles to filter by
  - `person_seniorities` (array): Seniority levels (owner, founder, c_suite, partner, vp, head, director, manager, senior, entry)
  - `organization_locations` (array): Company HQ locations
  - `organization_num_employees_ranges` (array): Employee count ranges
  - `person_locations` (array): Person locations
  - `q_keywords` (string): Keywords to search
  - `page` (integer): Page number (1-500)
  - `per_page` (integer): Results per page (max 100)

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
  api: "apollo",
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
        'api': 'apollo',
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
  -d '{"api": "apollo", "path": "/endpoint-path"}'
```
