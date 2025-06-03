import mysql.connector
from datetime import datetime

LOG_FILE = "/var/log/auth.log"

DB_CONFIG = {
    'host': 'localhost',
    'user': 'groot',       # Your MariaDB user
    'password': 'groot',   # Your MariaDB password
    'database': 'logs'
}

def parse_line(line):
    try:
        parts = line.strip().split(' ', 3)  # Split into max 4 parts
        if len(parts) < 4:
            return None

        timestamp_raw, hostname, service_raw, message = parts

        # Remove timezone offset (e.g. +09:00) for parsing
        if '+' in timestamp_raw:
            timestamp_raw = timestamp_raw.split('+')[0]
        elif '-' in timestamp_raw[20:]:
            timestamp_raw = timestamp_raw[:timestamp_raw.rfind('-')]

        timestamp = datetime.strptime(timestamp_raw, "%Y-%m-%dT%H:%M:%S.%f")

        service = service_raw.split('[')[0].rstrip(':')

        return {
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "hostname": hostname,
            "service": service,
            "message": message,
            "flag": "INFO"
        }

    except Exception:
        return None

def get_latest_timestamp(cursor):
    cursor.execute("SELECT MAX(timestamp) FROM auth_logs")
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def insert_log(entry, cursor):
    sql = """
    INSERT INTO auth_logs (timestamp, hostname, service, message, flag)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = (
        entry["timestamp"],
        entry["hostname"],
        entry["service"],
        entry["message"],
        entry["flag"]
    )
    cursor.execute(sql, values)

def import_auth_log():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        latest_ts = get_latest_timestamp(cursor)
        count = 0

        with open(LOG_FILE, 'r') as f:
            for line in f:
                entry = parse_line(line)
                if entry:
                    entry_ts = datetime.strptime(entry["timestamp"], "%Y-%m-%d %H:%M:%S")
                    if not latest_ts or entry_ts > latest_ts:
                        insert_log(entry, cursor)
                        count += 1

        conn.commit()
        print(f"✅ Imported {count} new log entries from auth.log")

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import_auth_log()
