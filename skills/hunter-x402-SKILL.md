---
name: hunter-x402
description: Call Hunter API via x402 direct USDC payment. Available endpoints: GET /v2/combined/find, GET /v2/people/find, GET /v2/email-count, POST /v2/discover, GET /v2/companies/find, GET /v2/domain-search, GET /v2/email-finder, GET /v2/email-verifier. Use for decentralized API payments. Requires PRIVATE_KEY with USDC on Base.
---

# Hunter (x402 Payment)

Hunter.io API for finding and verifying professional email addresses. Domain search, email finder, email verification, and company/person enrichment.

When the user asks to use Hunter with x402/crypto payment, use the code patterns below. The user's request is: $ARGUMENTS

**Base URL**: `https://x402.orth.sh/hunter`

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
  "https://x402.orth.sh/hunter/endpoint-path",  // Use appropriate endpoint
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
    "https://x402.orth.sh/hunter/endpoint-path",  # Use appropriate endpoint
    json={}  # Add body params if needed
)
print(response.json())
```

### How x402 Payment Works

1. Request is made to `https://x402.orth.sh/hunter/...`
2. Server returns 402 Payment Required with payment details
3. x402 SDK automatically creates and signs payment
4. Request is retried with X-PAYMENT header
5. Server verifies payment and returns API response
