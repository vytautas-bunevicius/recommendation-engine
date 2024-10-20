"""API module for handling recommendation-related endpoints."""

from typing import Any, List

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from src.database.connection import get_db_connection

router = APIRouter()

# Pydantic Models
class RecommendedMovie(BaseModel):
    id: str
    title: str
    genres: str
    avg_rating: float
    start_year: int

# Dependency to get the recommender
def get_recommender():
    from src.api.main import app  # Import here to avoid circular import
    return app.state.recommender

# Endpoints
@router.get("/users/{user_id}/recommendations", response_model=List[RecommendedMovie])
def get_recommendations(
    user_id: str,
    recommender = Depends(get_recommender),
) -> Any:
    """Get personalized movie recommendations for a user."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        recommendations = recommender.get_content_based_recommendations(cursor, user_id)
        return [RecommendedMovie(**movie) for movie in recommendations]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@router.get("/movies/{movie_id}/similar", response_model=List[RecommendedMovie])
def get_similar_movies(
    movie_id: str,
    limit: int = 10,
    recommender = Depends(get_recommender),
) -> Any:
    """Get movies similar to a given movie."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        similar_movies = recommender.get_similar_movies(cursor, movie_id, limit)
        return [RecommendedMovie(**movie) for movie in similar_movies]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
