"""API module for handling recommendation-related endpoints.

This module provides endpoints for retrieving personalized movie recommendations
for a user and fetching similar movies to a given movie.
"""

from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from src.database.connection import get_db_connection

router = APIRouter()


class RecommendedMovie(BaseModel):
    """Pydantic model representing a recommended movie."""

    id: str
    title: str
    genres: Optional[str] = None
    avg_rating: Optional[float] = None
    start_year: Optional[int] = None


def get_recommender():
    """Retrieve the content-based recommender system instance from the application state.

    Returns:
        ContentBasedRecommender: The recommender system instance.

    Raises:
        HTTPException: If the recommender system is not initialized.
    """
    from src.api.main import app  # Import here to avoid circular import
    if app.state.recommender is None:
        raise HTTPException(status_code=500, detail="Recommender system not initialized.")
    return app.state.recommender


@router.get("/users/{user_id}/recommendations", response_model=List[RecommendedMovie])
def get_recommendations(
    user_id: str,
    recommender=Depends(get_recommender),
) -> Any:
    """Get personalized movie recommendations for a user.

    Args:
        user_id (str): The unique identifier of the user.
        recommender (ContentBasedRecommender): The recommender system instance.

    Returns:
        List[RecommendedMovie]: A list of recommended movies for the user.

    Raises:
        HTTPException: If the recommender system or the database connection fails,
            or if no recommendations are found.
    """
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor()

    try:
        recommendations = recommender.get_content_based_recommendations(cursor, user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get recommendations: {e}") from e
    finally:
        cursor.close()
        conn.close()

    if not recommendations:
        raise HTTPException(status_code=404, detail="No recommendations found.")

    recommended_movies = [
        RecommendedMovie(
            id=movie["id"],
            title=movie["title"],
            genres=movie.get("genres"),
            avg_rating=movie.get("avg_rating"),
            start_year=movie.get("start_year"),
        )
        for movie in recommendations
    ]

    return recommended_movies


@router.get("/movies/{movie_id}/similar", response_model=List[RecommendedMovie])
def get_similar_movies(
    movie_id: str,
    limit: int = 10,
    recommender=Depends(get_recommender),
) -> Any:
    """Get movies similar to a given movie.

    Args:
        movie_id (str): The unique identifier of the movie.
        limit (int): The maximum number of similar movies to return. Defaults to 10.
        recommender (ContentBasedRecommender): The recommender system instance.

    Returns:
        List[RecommendedMovie]: A list of movies similar to the given movie.

    Raises:
        HTTPException: If the recommender system or the database connection fails,
            or if no similar movies are found.
    """
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor()

    try:
        similar_movies = recommender.get_similar_movies(cursor, movie_id, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get similar movies: {e}") from e
    finally:
        cursor.close()
        conn.close()

    if not similar_movies:
        raise HTTPException(status_code=404, detail="No similar movies found.")

    similar_movies_list = [
        RecommendedMovie(
            id=movie["id"],
            title=movie["title"],
            genres=movie.get("genres"),
            avg_rating=movie.get("avg_rating"),
            start_year=movie.get("start_year"),
        )
        for movie in similar_movies
    ]

    return similar_movies_list
