import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random

conn = psycopg2.connect(
    dbname="mydatabase",
    user="user",
    password="password",
    host="localhost"
)

cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM users")
count = cur.fetchone()[0]

if count == 0:
    fake = Faker()
    base_time = datetime.utcnow() - timedelta(days=30)

    for _ in range(100000):
        created = base_time + timedelta(days=random.randint(0, 30))
        updated = created + timedelta(hours=random.randint(0, 72))
        is_deleted = random.random() < 0.01

        cur.execute("""
            INSERT INTO users (name, email, created_at, updated_at, is_deleted)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            fake.name(),
            fake.unique.email(),
            created,
            updated,
            is_deleted
        ))

    conn.commit()

cur.close()
conn.close()