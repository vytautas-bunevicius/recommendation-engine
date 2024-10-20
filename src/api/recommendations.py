# src/api/recommendations.py

"""API module for handling recommendation-related endpoints."""

from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from src.database.connection import get_db_connection

router = APIRouter()

# Pydantic Models
class RecommendedMovie(BaseModel):
    id: str
    title: str
    genres: Optional[str] = None
    avg_rating: Optional[float] = None
    start_year: Optional[int] = None

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
    if recommender is None:
        raise HTTPException(status_code=500, detail="Recommender system not initialized.")

    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor()

    try:
        recommendations = recommender.get_content_based_recommendations(cursor, user_id)
    except Exception as e:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cursor.close()
    conn.close()

    if not recommendations:
        raise HTTPException(status_code=404, detail="No recommendations found.")

    # Convert to list of RecommendedMovie
    recommended_movies = []
    for movie in recommendations:
        movie_dict = {
            "id": movie['id'],  # Changed from movie[0] to movie['id']
            "title": movie['title'],  # Changed from movie[1] to movie['title']
            "genres": movie.get('genres'),  # Changed from movie[2] to movie['genres']
            "avg_rating": movie.get('avg_rating'),  # Changed from movie[3] to movie['avg_rating']
            "start_year": movie.get('start_year'),  # Changed from movie[4] to movie['start_year']
        }
        recommended_movies.append(movie_dict)

    return recommended_movies

@router.get("/movies/{movie_id}/similar", response_model=List[RecommendedMovie])
def get_similar_movies(
    movie_id: str,
    limit: int = 10,
    recommender = Depends(get_recommender),
) -> Any:
    """Get movies similar to a given movie."""
    if recommender is None:
        raise HTTPException(status_code=500, detail="Recommender system not initialized.")

    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor()

    try:
        similar_movies = recommender.get_similar_movies(cursor, movie_id, limit)
    except Exception as e:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cursor.close()
    conn.close()

    if not similar_movies:
        raise HTTPException(status_code=404, detail="No similar movies found.")

    # Convert to list of RecommendedMovie
    similar_movies_list = []
    for movie in similar_movies:
        movie_dict = {
            "id": movie['id'],  # Changed from movie[0] to movie['id']
            "title": movie['title'],  # Changed from movie[1] to movie['title']
            "genres": movie.get('genres'),  # Changed from movie[2] to movie['genres']
            "avg_rating": movie.get('avg_rating'),  # Changed from movie[3] to movie['avg_rating']
            "start_year": movie.get('start_year'),  # Changed from movie[4] to movie['start_year']
        }
        similar_movies_list.append(movie_dict)

    return similar_movies_list
