import os
from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy.orm import Session
from app.database import get_db
from app.exports.jobs import executor
from app.exports.service import run_export

router = APIRouter()

@router.post("/exports/full", status_code=202)
def full_export(request: Request, db: Session = Depends(get_db)):
    consumer_id = request.headers.get("X-Consumer-ID")
    if not consumer_id:
        raise HTTPException(status_code=400, detail="Missing X-Consumer-ID")

    executor.submit(run_export, db, consumer_id, "full", "output")

    return {
        "status": "started",
        "exportType": "full"
    }


@router.post("/exports/incremental", status_code=202)
def incremental_export(request: Request, db: Session = Depends(get_db)):
    consumer_id = request.headers.get("X-Consumer-ID")
    if not consumer_id:
        raise HTTPException(status_code=400, detail="Missing X-Consumer-ID")

    executor.submit(run_export, db, consumer_id, "incremental", "output")

    return {
        "status": "started",
        "exportType": "incremental"
    }


@router.post("/exports/delta", status_code=202)
def delta_export(request: Request, db: Session = Depends(get_db)):
    consumer_id = request.headers.get("X-Consumer-ID")
    if not consumer_id:
        raise HTTPException(status_code=400, detail="Missing X-Consumer-ID")

    executor.submit(run_export, db, consumer_id, "delta", "output")

    return {
        "status": "started",
        "exportType": "delta"
    }