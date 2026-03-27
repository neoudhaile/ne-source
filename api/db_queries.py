from datetime import datetime, timezone


def create_run(conn) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO pipeline_runs (triggered_by) VALUES ('ui') RETURNING id"
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        cur.close()


def update_run(conn, run_id: int, **fields):
    if not fields:
        return
    set_clause = ', '.join(f'{k} = %s' for k in fields)
    values = list(fields.values()) + [run_id]
    cur = conn.cursor()
    try:
        cur.execute(
            f'UPDATE pipeline_runs SET {set_clause} WHERE id = %s',
            values
        )
        conn.commit()
    finally:
        cur.close()


def get_runs(conn, limit: int = 20) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            '''SELECT id, started_at, finished_at, status, inserted,
                      skipped_geo, skipped_dupe, total_leads, error_message,
                      triggered_by, cost
               FROM pipeline_runs
               ORDER BY started_at DESC
               LIMIT %s''',
            (limit,)
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    finally:
        cur.close()


def get_stats(conn) -> dict:
    cur = conn.cursor()
    try:
        cur.execute('SELECT COUNT(*) FROM smb_leads')
        total = cur.fetchone()[0]

        cur.execute(
            '''SELECT industry, COUNT(*) AS count
               FROM smb_leads
               GROUP BY industry
               ORDER BY count DESC'''
        )
        by_industry = [
            {'industry': row[0], 'count': row[1]}
            for row in cur.fetchall()
        ]

        cur.execute(
            '''SELECT COALESCE(ownership_type, 'Unknown') AS ownership_type,
                      COUNT(*) AS count
               FROM smb_leads
               GROUP BY ownership_type
               ORDER BY count DESC'''
        )
        by_ownership = [
            {'ownership_type': row[0], 'count': row[1]}
            for row in cur.fetchall()
        ]

        return {
            'total_leads': total,
            'by_industry': by_industry,
            'by_ownership_type': by_ownership,
        }
    finally:
        cur.close()
