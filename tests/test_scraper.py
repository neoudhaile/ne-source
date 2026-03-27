import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.scraper import search_businesses

results = search_businesses(
    query='HVAC repair',
    city='Los Angeles, CA',
    page_size=3,
    min_rating=3.5,
    min_reviews=5
)

if results:
    print(json.dumps(results[0], indent=2))
