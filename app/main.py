from fastapi import FastAPI
from app.exports.router import router as export_router
from app.database import SessionLocal
from app.models import User
from faker import Faker
from datetime import datetime, timedelta
import random
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = FastAPI()

app.include_router(export_router)


# ---------------------------
# SEED FUNCTION
# ---------------------------
def seed_if_empty():
    print("Seed function started")

    # Wait a bit to ensure DB is fully ready
    time.sleep(3)

    db = SessionLocal()
    try:
        count = db.query(User).count()
        print(f"Current user count: {count}")

        if count == 0:
            print("Seeding database with 100,000 users...")

            fake = Faker()
            base_time = datetime.utcnow() - timedelta(days=30)

            users = []
            for _ in range(100000):
                created = base_time + timedelta(days=random.randint(0, 30))
                updated = created + timedelta(hours=random.randint(0, 72))
                is_deleted = random.random() < 0.01

                users.append(User(
                    name=fake.name(),
                    email=fake.unique.email(),
                    created_at=created,
                    updated_at=updated,
                    is_deleted=is_deleted
                ))

            db.bulk_save_objects(users)
            db.commit()

            print("Database seeded successfully.")

    except Exception as e:
        print("Seeding error:", e)

    finally:
        db.close()


# ---------------------------
# STARTUP EVENT
# ---------------------------
@app.on_event("startup")
def startup_event():
    print("App startup event triggered")
    seed_if_empty()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat()
    }