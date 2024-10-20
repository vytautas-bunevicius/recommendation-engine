"""API module for handling movie-related endpoints."""

from typing import Any, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.database.connection import get_db_connection

router = APIRouter()

# Pydantic Models
class Movie(BaseModel):
    id: str
    title: str
    original_title: str
    type: str
    start_year: int
    runtime: int
    genres: str
    avg_rating: float = None
    num_votes: int = None

# Endpoints
@router.get("/", response_model=List[Movie])
def get_movies(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort: str = Query("popularity"),
    order: str = Query("desc"),
    type: str = Query("movie"),
    min_votes: int = Query(1000, ge=0),
) -> Any:
    """Retrieve a list of movies."""
    sort_options = {
        'popularity': 'mr.num_votes',
        'rating': 'mr.avg_rating',
        'year': 'm.start_year',
    }
    sort_column = sort_options.get(sort, 'mr.num_votes')
    order = order.upper()

    conn = get_db_connection()
    cursor = conn.cursor()

    query = f"""
        SELECT m.id, m.title, m.original_title, m.type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.type = %s AND COALESCE(mr.num_votes, 0) >= %s
        ORDER BY {sort_column} {order} NULLS LAST
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (type, min_votes, limit, offset))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return movies

@router.get("/{movie_id}", response_model=Movie)
def get_movie(movie_id: str) -> Any:
    """Retrieve details of a specific movie."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT m.*, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.id = %s
    """, (movie_id,))
    movie = cursor.fetchone()

    cursor.close()
    conn.close()

    if movie:
        return movie
    raise HTTPException(status_code=404, detail="Movie not found")

@router.get("/search", response_model=List[Movie])
def search_movies(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    type: str = Query("movie"),
    min_votes: int = Query(100, ge=0),
) -> Any:
    """Search for movies by title."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT m.id, m.title, m.original_title, m.type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.title ILIKE %s AND m.type = %s AND COALESCE(mr.num_votes, 0) >= %s
        ORDER BY COALESCE(mr.num_votes, 0) DESC NULLS LAST
        LIMIT %s OFFSET %s
        """,
        (f'%{query}%', type, min_votes, limit, offset),
    )
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return movies
