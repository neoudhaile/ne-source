---
name: sixtyfour-x402
description: Call Sixtyfour API API via x402 direct USDC payment. Available endpoints: POST /enrich-company, POST /enrich-lead, POST /find-phone, POST /find-email. Use for decentralized API payments. Requires PRIVATE_KEY with USDC on Base.
---

# Sixtyfour API (x402 Payment)

Build custom research agents to enrich people and company data, and surface real-time signals all with a simple API call.

When the user asks to use Sixtyfour API with x402/crypto payment, use the code patterns below. The user's request is: $ARGUMENTS

**Base URL**: `https://x402.orth.sh/sixtyfour`

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
  "https://x402.orth.sh/sixtyfour/endpoint-path",  // Use appropriate endpoint
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
    "https://x402.orth.sh/sixtyfour/endpoint-path",  # Use appropriate endpoint
    json={}  # Add body params if needed
)
print(response.json())
```

### How x402 Payment Works

1. Request is made to `https://x402.orth.sh/sixtyfour/...`
2. Server returns 402 Payment Required with payment details
3. x402 SDK automatically creates and signs payment
4. Request is retried with X-PAYMENT header
5. Server verifies payment and returns API response
