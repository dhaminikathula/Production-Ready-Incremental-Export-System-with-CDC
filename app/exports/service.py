import os
import csv
import uuid
import logging
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import User, Watermark
from app.database import SessionLocal

logger = logging.getLogger(__name__)


# ============================
# MAIN EXPORT RUNNER
# ============================

def run_export(consumer_id: str, export_type: str, output_dir: str):

    db = SessionLocal()

    try:
        with db.begin():

            rows = fetch_rows(db, consumer_id, export_type)

            filename = f"{export_type}_{consumer_id}_{int(datetime.utcnow().timestamp())}.csv"
            filepath = os.path.join(output_dir, filename)

            if export_type == "delta":
                write_delta_csv(rows, filepath)
            else:
                write_standard_csv(rows, filepath)

            if rows:
                max_ts = max(r.updated_at for r in rows)
                upsert_watermark(db, consumer_id, max_ts)

        print(f"Export completed for {consumer_id}")

    except Exception as e:
        print("EXPORT ERROR:", e)

    finally:
        db.close()
# ============================
# FETCH LOGIC
# ============================

def fetch_rows(session: Session, consumer_id: str, export_type: str):

    if export_type == "full":
        return session.scalars(
            select(User).where(User.is_deleted == False)
        ).all()

    watermark = get_watermark(session, consumer_id)

    if watermark:
        condition = User.updated_at > watermark.last_exported_at
    else:
        condition = True  # No watermark → full snapshot

    if export_type == "incremental":
        return session.scalars(
            select(User).where(
                condition,
                User.is_deleted == False
            )
        ).all()

    elif export_type == "delta":
        return session.scalars(
            select(User).where(condition)
        ).all()

    else:
        raise ValueError("Invalid export type")


# ============================
# CSV WRITERS
# ============================

def write_standard_csv(rows, filepath):

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "id",
            "name",
            "email",
            "created_at",
            "updated_at",
            "is_deleted"
        ])

        for row in rows:
            writer.writerow([
                row.id,
                row.name,
                row.email,
                row.created_at,
                row.updated_at,
                row.is_deleted
            ])


def write_delta_csv(rows, filepath):

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)

        writer.writerow([
            "operation",
            "id",
            "name",
            "email",
            "created_at",
            "updated_at",
            "is_deleted"
        ])

        for row in rows:
            writer.writerow([
                determine_operation(row),
                row.id,
                row.name,
                row.email,
                row.created_at,
                row.updated_at,
                row.is_deleted
            ])


# ============================
# DELTA OPERATION LOGIC
# ============================

def determine_operation(user):

    if user.is_deleted:
        return "DELETE"
    elif user.created_at == user.updated_at:
        return "INSERT"
    else:
        return "UPDATE"


# ============================
# WATERMARK MANAGEMENT
# ============================

def get_watermark(session: Session, consumer_id: str):
    return session.scalar(
        select(Watermark).where(Watermark.consumer_id == consumer_id)
    )


def upsert_watermark(session: Session, consumer_id: str, last_exported_at):

    existing = get_watermark(session, consumer_id)

    if existing:
        existing.last_exported_at = last_exported_at
        existing.updated_at = datetime.utcnow()
    else:
        session.add(
            Watermark(
                consumer_id=consumer_id,
                last_exported_at=last_exported_at,
                updated_at=datetime.utcnow()
            )
        )