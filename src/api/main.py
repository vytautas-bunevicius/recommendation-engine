"""Main module for the Movie Recommender API.

This module initializes the FastAPI application, sets up middleware, includes API routers,
mounts static files, serves the frontend, and initializes the content-based recommender system
on startup.
"""

import logging
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import DictCursor

from src.api.movies import router as movies_router
from src.api.recommendations import router as recommendations_router
from src.api.users import router as users_router
from src.database.connection import get_db_connection
from src.recommender.content_based import ContentBasedRecommender

app = FastAPI(
    title="Movie Recommender",
    description="API for movie recommendations using FastAPI",
    version="1.0.0",
)

ORIGINS = [
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(movies_router, prefix="/movies", tags=["Movies"])
app.include_router(users_router, prefix="/users", tags=["Users"])
app.include_router(recommendations_router, tags=["Recommendations"])

CURRENT_DIR = Path(__file__).resolve().parent
UI_DIR = CURRENT_DIR.parent / "ui"

app.mount("/static", StaticFiles(directory=str(UI_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def serve_ui():
    """Serve the frontend UI.

    Returns:
        FileResponse: The index.html file of the frontend.
    """
    return FileResponse(str(UI_DIR / "index.html"))


@app.on_event("startup")
def startup_event():
    """Initialize the content-based recommender system on application startup.

    Raises:
        RuntimeError: If the database connection fails.
        Exception: If any other error occurs during initialization.
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            raise RuntimeError("Failed to connect to the database.")
        cursor = conn.cursor(cursor_factory=DictCursor)
        app.state.recommender = ContentBasedRecommender(cursor)
        logging.info("ContentBasedRecommender initialized successfully.")
    except Exception as e:
        logging.error("Error initializing ContentBasedRecommender: %s", e)
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    """Run the FastAPI application."""
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
