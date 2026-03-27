---
name: apollo-x402
description: Call Apollo API API via x402 direct USDC payment. Available endpoints: GET /api/v1/organizations/enrich, POST /api/v1/organizations/bulk_enrich, GET /api/v1/organizations/{organization_id}/job_postings, POST /api/v1/news_articles/search, GET /api/v1/organizations/{id}, POST /api/v1/people/match, POST /api/v1/people/bulk_match, POST /api/v1/mixed_companies/search, POST /api/v1/mixed_people/api_search. Use for decentralized API payments. Requires PRIVATE_KEY with USDC on Base.
---

# Apollo API (x402 Payment)

Apollo.io API for people and company enrichment, search, and prospecting. Access the Apollo database of 210M+ contacts and 30M+ companies.

When the user asks to use Apollo API with x402/crypto payment, use the code patterns below. The user's request is: $ARGUMENTS

**Base URL**: `https://x402.orth.sh/apollo`

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

Set the environment variable (keep your private key secure):
```bash
export PRIVATE_KEY=0x...your_wallet_private_key
```

Requires USDC on Base blockchain.

### JavaScript/TypeScript (x402-fetch)

```javascript
import { wrapFetchWithPayment } from "x402-fetch";
import { privateKeyToAccount } from "viem/accounts";

const account = privateKeyToAccount(process.env.PRIVATE_KEY);
const fetchWithPayment = wrapFetchWithPayment(fetch, account);

const response = await fetchWithPayment(
  "https://x402.orth.sh/apollo/endpoint-path",  // Use appropriate endpoint
  {
    method: "POST",  // Use appropriate method
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ /* body params */ })
  }
);

const data = await response.json();
console.log(data);
```

### Python (requests + x402)

```python
import os
import requests
from eth_account import Account
from x402.clients.requests import x402_http_adapter

account = Account.from_key(os.getenv("PRIVATE_KEY"))
session = requests.Session()
session.mount("https://", x402_http_adapter(account))

response = session.post(
    "https://x402.orth.sh/apollo/endpoint-path",  # Use appropriate endpoint
    json={}  # Add body params if needed
)
print(response.json())
```

### How x402 Payment Works

1. Request is made to `https://x402.orth.sh/apollo/...`
2. Server returns 402 Payment Required with payment details
3. x402 SDK automatically creates and signs payment
4. Request is retried with X-PAYMENT header
5. Server verifies payment and returns API response
