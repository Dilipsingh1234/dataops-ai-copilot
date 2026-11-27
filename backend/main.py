from fastapi import FastAPI, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from backend.database import check_database


app = FastAPI(openapi_url=None)


@app.get("/health/db")
def database_health() -> dict[str, str]:
    try:
        healthy = check_database()
    except SQLAlchemyError:
        healthy = False

    if not healthy:
        raise HTTPException(status_code=503, detail="Database unavailable")

    return {"database": "ok"}
