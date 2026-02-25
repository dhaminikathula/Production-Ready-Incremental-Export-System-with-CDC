from fastapi import FastAPI
from app.exports.router import router as export_router
from datetime import datetime

app = FastAPI()

app.include_router(export_router)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat()
    }