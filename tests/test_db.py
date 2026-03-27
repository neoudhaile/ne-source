import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline.db import get_connection, count_leads

conn = get_connection()
print(f'Lead count: {count_leads(conn)}')
conn.close()
print('DB OK')
