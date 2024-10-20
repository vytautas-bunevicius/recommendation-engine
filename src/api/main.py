# src/api/main.py

import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from psycopg2.extras import DictCursor

from src.api.movies import router as movies_router
from src.api.users import router as users_router
from src.api.recommendations import router as recommendations_router
from src.recommender.content_based import ContentBasedRecommender
from src.database.connection import get_db_connection

# Initialize FastAPI app
app = FastAPI(
    title="Movie Recommender",
    description="API for movie recommendations using FastAPI",
    version="1.0.0",
)

# Configure CORS
origins = [
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Update as per deployment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers without redundant prefixes
app.include_router(movies_router, prefix="/movies", tags=["Movies"])
app.include_router(users_router, prefix="/users", tags=["Users"])
app.include_router(recommendations_router, tags=["Recommendations"])  # Removed prefix

# Mount static files
current_dir = Path(__file__).resolve().parent  # src/api
ui_dir = current_dir.parent / "ui"  # src/ui

# Mount static files at /static to serve JS and CSS
app.mount("/static", StaticFiles(directory=str(ui_dir)), name="static")

# Mount index.html at root
@app.get("/", include_in_schema=False)
async def serve_ui():
    return FileResponse(str(ui_dir / "index.html"))

# Initialize ContentBasedRecommender on startup
@app.on_event("startup")
def startup_event():
    """Initialize the content-based recommender system."""
    try:
        conn = get_db_connection()
        if conn is None:
            raise RuntimeError("Failed to connect to the database.")
        cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor for dictionary-like access
        app.state.recommender = ContentBasedRecommender(cursor)
        logging.info("ContentBasedRecommender initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing ContentBasedRecommender: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)
