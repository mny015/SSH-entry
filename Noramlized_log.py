import re
import pymysql
from datetime import datetime

# Connect to MariaDB
conn = pymysql.connect(
    host='localhost',
    user='groot',
    password='groot',
    database='logs'
)
cursor = conn.cursor()

log_path = '/var/log/auth.log'

log_pattern = re.compile(
    r'^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}) (?P<host>\S+) (?P<service>[\w-]+)(?:\[(?P<session_id>\d+)\])?: (?P<message>.+)$'
)

def get_or_insert(table, column, value):
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(f"INSERT INTO {table} ({column}) VALUES (%s)", (value,))
    conn.commit()
    return cursor.lastrowid

inserted_count = 0

with open(log_path, 'r') as file:
    for line in file:
        match = log_pattern.match(line)
        if match:
            try:
                timestamp = datetime.strptime(match.group('timestamp'), '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                continue

            hostname = match.group('host')
            service_name = match.group('service')
            message = match.group('message')
            session_id = match.group('session_id')
            flag = None

            if 'Failed password' in message:
                flag = 'failed_password'
            elif 'Accepted password' in message:
                flag = 'accepted_password'
            elif 'session opened' in message:
                flag = 'session_opened'
            elif 'session closed' in message:
                flag = 'session_closed'

            host_id = get_or_insert('hosts', 'hostname', hostname)
            service_id = get_or_insert('services', 'service_name', service_name)

            # Check for duplicate
            cursor.execute("""
                SELECT id FROM auth_logs
                WHERE timestamp = %s AND host_id = %s AND service_id = %s AND message = %s
            """, (timestamp.strftime('%Y-%m-%d %H:%M:%S'), host_id, service_id, message))

            if cursor.fetchone():
                continue  # Skip duplicate

            # Insert if not duplicate
            cursor.execute("""
                INSERT INTO auth_logs (timestamp, host_id, service_id, message, flag, session_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (timestamp.strftime('%Y-%m-%d %H:%M:%S'), host_id, service_id, message, flag, session_id))
            inserted_count += 1

conn.commit()
cursor.close()
conn.close()

print(f"✅ {inserted_count} unique log entries inserted.")
