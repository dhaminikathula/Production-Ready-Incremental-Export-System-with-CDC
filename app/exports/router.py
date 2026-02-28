from fastapi import APIRouter, Header
from app.exports.jobs import executor
from app.exports.service import run_export

router = APIRouter()

@router.post("/exports/full", status_code=202)
def full_export(consumer_id: str = Header(..., alias="X-Consumer-ID")):
    executor.submit(run_export, consumer_id, "full", "output")
    return {"status": "started", "exportType": "full"}

@router.post("/exports/incremental", status_code=202)
def incremental_export(consumer_id: str = Header(..., alias="X-Consumer-ID")):
    executor.submit(run_export, consumer_id, "incremental", "output")
    return {"status": "started", "exportType": "incremental"}

@router.post("/exports/delta", status_code=202)
def delta_export(consumer_id: str = Header(..., alias="X-Consumer-ID")):
    executor.submit(run_export, consumer_id, "delta", "output")
    return {"status": "started", "exportType": "delta"}