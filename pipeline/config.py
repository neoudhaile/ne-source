SEARCH_PROVIDER = 'openmart'

ENRICH_CONCURRENCY = 3
ENRICH_PHASE2_CONCURRENCY = 3
ENABLE_REVIEW_SCRAPE = False
FAST_TIMEOUT = 20
SLOW_TIMEOUT = 35
SCRAPE_DIRECT_TIMEOUT = 12
SCRAPE_ZYTE_TIMEOUT = 30
SCRAPE_MIN_TEXT_LENGTH = 500
SCRAPE_ENABLE_ZYTE_FALLBACK = True
SCRAPE_BLOCK_PATTERNS = [
    'access denied',
    'forbidden',
    'captcha',
    'verify you are human',
    'enable javascript',
    'cloudflare',
]

INDUSTRIES = [
    'Car Wash',
]

CITIES = [
    'Los Angeles, CA',
]

MIN_REVIEWS      = 5
MIN_RATING       = 3.5
MAX_LEADS_PER_RUN = 1

LA_LAT           = 34.0522
LA_LNG           = -118.2437
GEO_RADIUS_MILES = 40

# Notion export
ENABLE_NOTION_EXPORT = True
NOTION_DATABASE_ID = ''

# Owner-contact benchmark / CSV enrichment domain recovery
ENABLE_DOMAIN_RECOVERY = True
ENABLE_OPENMART_DOMAIN_RECOVERY = True
ENABLE_OPENMART_COMPANY_ENRICH = True
DOMAIN_RECOVERY_TIMEOUT = 15
DOMAIN_VERIFICATION_MIN_TOKEN_OVERLAP = 1

TARGET_CITIES = {
    'los angeles', 'long beach', 'pasadena', 'burbank', 'glendale',
    'torrance', 'santa monica', 'anaheim', 'compton', 'inglewood',
    'hawthorne', 'el segundo', 'manhattan beach', 'redondo beach',
    'hermosa beach', 'culver city', 'west hollywood', 'beverly hills',
    'encino', 'van nuys', 'reseda', 'chatsworth', 'canoga park',
    'woodland hills', 'thousand oaks', 'calabasas', 'malibu',
    'pomona', 'ontario', 'rancho cucamonga', 'riverside',
}
