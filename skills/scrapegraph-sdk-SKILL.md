---
name: scrapegraph
description: Call Scrapegraphai API API via Orthogonal. Available endpoints: GET /v1/searchscraper/{request_id}, GET /v1/markdownify/{request_id}, POST /v1/sitemap, POST /v1/markdownify, GET /v1/smartscraper/{request_id}, POST /v1/searchscraper, POST /v1/smartscraper, POST /v1/crawl, GET /v1/crawl/{task_id}, POST /v1/scrape, GET /v1/sitemap/{request_id}. Use when working with Scrapegraphai API or when user asks to The ScrapeGraphAI API provides powerful endpoints for AI-powered web scraping and content extraction. Our RESTful API allows you to extract structured data from any website, perform AI-powered web searches, and convert web pages to clean markdown.. Requires ORTHOGONAL_API_KEY.
---

# Scrapegraphai API

The ScrapeGraphAI API provides powerful endpoints for AI-powered web scraping and content extraction. Our RESTful API allows you to extract structured data from any website, perform AI-powered web searches, and convert web pages to clean markdown.

When the user asks to use Scrapegraphai API or any of its endpoints, use the code patterns below. The user's request is: $ARGUMENTS

## Available Endpoints

### GET /v1/searchscraper/{request_id}
Get the status and results of a previous search request
- **Price**: Free



### GET /v1/markdownify/{request_id}
Check the status and retrieve results of a Markdownify request.
- **Price**: Free



### POST /v1/sitemap
Extract all URLs from a website sitemap automatically.
- **Price**: Dynamic

- **Body params**:
  - `website_url` (string, required): The URL of the website you want to extract the sitemap from. The API will automatically locate the sitemap.xml file.
  - `headers` (object): Optional headers to customize the request behavior. This can include user agent, cookies, or other HTTP headers.
  - `mock` (boolean): Optional parameter to enable mock mode. When set to true, the request will return mock data instead of performing an actual extraction. Useful for testing and development.
  - `stealth` (boolean): Optional parameter to enable stealth mode. When set to true, the scraper will use advanced anti-detection techniques to bypass bot protection and access protected websites. Adds +4 credits to the request cost.

### POST /v1/markdownify
Convert any webpage into clean, readable Markdown format.
- **Price**: Dynamic

- **Body params**:
  - `website_url` (string, required): The URL of the webpage you want to convert to markdown.
  - `headers` (object): Optional headers to send with the request, including cookies and user agent
  - `stealth` (boolean): Enable stealth mode to bypass bot protection using advanced anti-detection techniques. Adds +4 credits to the request cost

### GET /v1/smartscraper/{request_id}
Check the status and retrieve results of a SmartScraper request.
- **Price**: Free



### POST /v1/searchscraper
Start a new AI-powered web search request
- **Price**: Dynamic

- **Body params**:
  - `user_prompt` (string, required): The search query or question you want to ask. This should be a clear and specific prompt that will guide the AI in finding and extracting relevant information. Example: “What is the latest version of Python and what are its main features?”
  - `headers` (object): Optional headers to customize the search behavior. This can include user agent, cookies, or other HTTP headers. Example: {   "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",   "Cookie": "cookie1=value1; cookie2=value2" }
  - `output_schema` (object): Optional schema to structure the output. If provided, the AI will attempt to format the results according to this schema. Example: {   "properties": {     "version": {"type": "string"},     "release_date": {"type": "string"},     "major_features": {"type": "array", "items": {"type": "string"}}   },   "required": ["version", "release_date", "major_features"] }
  - `mock` (string): Optional parameter to enable mock mode. When set to true, the request will return mock data instead of performing an actual search. Useful for testing and development. Default: false
  - `stealth` (boolean): Optional parameter to enable stealth mode. When set to true, the scraper will use advanced anti-detection techniques to bypass bot protection and access protected websites. Adds +4 credits to the request cost. Default: false

### POST /v1/smartscraper
Extract content from a webpage using AI by providing a natural language prompt and a URL.
- **Price**: Dynamic

- **Body params**:
  - `user_prompt` (string, required): Natural language description of what information you want to extract from the webpage.
  - `website_url` (string, required): The URL of the webpage you want to extract information from. You must provide exactly one of: website_url, website_html, or website_markdown.
  - `website_html` (string): Raw HTML content to process directly (max 2MB). Mutually exclusive with website_url and website_markdown. Useful when you already have HTML content cached or want to process modified HTML.
  - `headers` (object): Optional custom HTTP headers to send with the request. Useful for setting User-Agent, cookies, authentication tokens, and other request metadata. Example: {"User-Agent": "Mozilla/5.0...", "Cookie": "session=abc123"}
  - `output_schema` (object): Optional schema to structure the output. If provided, the AI will attempt to format the results according to this schema.
  - `stealth` (boolean): Enable stealth mode to bypass bot protection using advanced anti-detection techniques. Adds +4 credits to the request cost
  - ` website_markdown` (string): Raw Markdown content to process directly (max 2MB). Mutually exclusive with website_url and website_html. Perfect for extracting structured data from Markdown documentation, README files, or any content already in Markdown format.
  - `total_pages` (number): Optional parameter to enable pagination and scrape multiple pages. Specify the number of pages to extract data from. Default: 1 Range: 1-100
  - ` number_of_scrolls` (number): Optional parameter for infinite scroll pages. Specify how many times to scroll down to load more content before extraction. Default: 0 Range: 0-50
  - ` render_heavy_js` (boolean): Optional parameter to enable enhanced JavaScript rendering for heavy JS websites (React, Vue, Angular, SPAs). Use when standard rendering doesn’t capture all content. Default: false
  - ` mock` (boolean): Optional parameter to enable mock mode. When set to true, the request will return mock data instead of performing an actual extraction. Useful for testing and development. Default: false
  - ` cookies` (object): Optional cookies object for authentication and session management. Useful for accessing authenticated pages or maintaining session state. Example: {"session_id": "abc123", "auth_token": "xyz789"}
  - ` steps` (array): Optional array of interaction steps to perform on the webpage before extraction. Each step is a string describing the action to take (e.g., “click on filter button”, “wait for results to load”). Example: ["click on search button", "type query in search box", "wait for results"]

### POST /v1/crawl
Start a new web crawl request with AI extraction or markdown conversion
- **Price**: Dynamic

- **Body params**:
  - `url` (string, required): 
  - `prompt` (string): 
  - `extraction_mode` (boolean): 
  - `cache_website` (boolean): 
  - `depth` (number): 
  - `max_pages` (number): 
  - `same_domain_only` (boolean): 
  - `batch_size` (integer): 
  - `schema` (object): 
  - `rules` (object): 
  - `sitemap` (string): 
  - `render_heavy_js` (string): 
  - `stealth` (string): 

### GET /v1/crawl/{task_id}
Get the status and results of a previous smartcrawl request
- **Price**: Free



### POST /v1/scrape
Extract raw HTML content from web pages with JavaScript rendering support
- **Price**: Dynamic

- **Body params**:
  - `website_url` (string, required): The URL of the webpage to scrape. Example: "https://example.com"
  - `render_heavy_js` (boolean): Set to true for heavy JavaScript rendering. Default: false
  - `branding` (boolean): Return extracted brand design and metadata. Default: false
  - `stealth` (string): Enable stealth mode for anti-bot protection. Adds additional credits. Default: false

### GET /v1/sitemap/{request_id}
Check the status and retrieve results of a Sitemap request.
- **Price**: Free



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
  api: "scrapegraph",
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
        'api': 'scrapegraph',
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
  -d '{"api": "scrapegraph", "path": "/endpoint-path"}'
```
