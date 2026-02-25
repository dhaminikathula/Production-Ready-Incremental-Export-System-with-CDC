import os
import csv
import uuid
import logging
from datetime import datetime
from sqlalchemy import select, func
from sqlalchemy.orm import Session
from app.models import User, Watermark

logger = logging.getLogger(__name__)


# ----------------------------
# PUBLIC ENTRY FUNCTION
# ----------------------------

def run_export(session: Session, consumer_id: str, export_type: str, output_dir: str):
    """
    Main export runner.
    Handles:
    - full
    - incremental
    - delta
    """

    job_id = str(uuid.uuid4())
    start_time = datetime.utcnow()

    filename = f"{export_type}_{consumer_id}_{int(start_time.timestamp())}.csv"
    filepath = os.path.join(output_dir, filename)

    logger.info("Export job started", extra={
        "jobId": job_id,
        "consumerId": consumer_id,
        "exportType": export_type
    })

    try:
        with session.begin():

            # 1️⃣ Fetch data
            rows = fetch_rows(session, consumer_id, export_type)

            # 2️⃣ Write CSV
            if export_type == "delta":
                write_delta_csv(rows, filepath)
            else:
                write_standard_csv(rows, filepath)

            # 3️⃣ Update watermark ONLY after successful write
            if rows:
                max_timestamp = max(row.updated_at for row in rows)
                upsert_watermark(session, consumer_id, max_timestamp)

        duration = (datetime.utcnow() - start_time).total_seconds()

        logger.info("Export job completed", extra={
            "jobId": job_id,
            "rowsExported": len(rows),
            "durationSeconds": duration
        })

        return {
            "jobId": job_id,
            "status": "completed",
            "outputFilename": filename
        }

    except Exception as e:
        logger.error("Export job failed", extra={
            "jobId": job_id,
            "error": str(e)
        })
        raise


# ----------------------------
# FETCH LOGIC
# ----------------------------

def fetch_rows(session: Session, consumer_id: str, export_type: str):

    if export_type == "full":
        return session.scalars(
            select(User).where(User.is_deleted == False)
        ).all()

    watermark = get_watermark(session, consumer_id)

    if watermark:
        condition = User.updated_at > watermark.last_exported_at
    else:
        condition = True  # No watermark yet → full snapshot

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


# ----------------------------
# CSV WRITERS
# ----------------------------

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
            operation = determine_operation(row)

            writer.writerow([
                operation,
                row.id,
                row.name,
                row.email,
                row.created_at,
                row.updated_at,
                row.is_deleted
            ])


# ----------------------------
# DELTA OPERATION LOGIC
# ----------------------------

def determine_operation(user):

    if user.is_deleted:
        return "DELETE"
    elif user.created_at == user.updated_at:
        return "INSERT"
    else:
        return "UPDATE"


# ----------------------------
# WATERMARK MANAGEMENT
# ----------------------------

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
        new_watermark = Watermark(
            consumer_id=consumer_id,
            last_exported_at=last_exported_at,
            updated_at=datetime.utcnow()
        )
        session.add(new_watermark)