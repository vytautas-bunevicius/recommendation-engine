# src/api/movies.py

"""API module for handling movie-related endpoints."""

from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.database.connection import get_db_connection

router = APIRouter()

# Pydantic Models
class Movie(BaseModel):
    id: str
    title: str
    original_title: Optional[str]
    type: str
    start_year: Optional[int]
    runtime: Optional[int]
    genres: Optional[str]
    avg_rating: Optional[float] = None
    num_votes: Optional[int] = None

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
    if order not in ['ASC', 'DESC']:
        raise HTTPException(status_code=400, detail="Invalid order parameter. Use 'asc' or 'desc'.")

    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
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
    try:
        cursor.execute(query, (type, min_votes, limit, offset))
        movies = cursor.fetchall()
    except Exception as e:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cursor.close()
    conn.close()

    # Convert to list of dictionaries
    movie_list = []
    for movie in movies:
        movie_dict = {
            "id": movie[0],
            "title": movie[1],
            "original_title": movie[2],
            "type": movie[3],
            "start_year": movie[4],
            "runtime": movie[5],
            "genres": movie[6],
            "avg_rating": movie[7],
            "num_votes": movie[8],
        }
        movie_list.append(movie_dict)

    return movie_list

@router.get("/{movie_id}", response_model=Movie)
def get_movie(movie_id: str) -> Any:
    """Retrieve details of a specific movie."""
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor()

    query = """
        SELECT m.id, m.title, m.original_title, m.type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.id = %s
    """
    try:
        cursor.execute(query, (movie_id,))
        movie = cursor.fetchone()
    except Exception as e:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cursor.close()
    conn.close()

    if movie:
        movie_dict = {
            "id": movie[0],
            "title": movie[1],
            "original_title": movie[2],
            "type": movie[3],
            "start_year": movie[4],
            "runtime": movie[5],
            "genres": movie[6],
            "avg_rating": movie[7],
            "num_votes": movie[8],
        }
        return movie_dict
    else:
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
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor()

    search_query = f"%{query}%"
    sql = """
        SELECT m.id, m.title, m.original_title, m.type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.title ILIKE %s AND m.type = %s AND COALESCE(mr.num_votes, 0) >= %s
        ORDER BY COALESCE(mr.num_votes, 0) DESC NULLS LAST
        LIMIT %s OFFSET %s
    """
    try:
        cursor.execute(sql, (search_query, type, min_votes, limit, offset))
        movies = cursor.fetchall()
    except Exception as e:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    cursor.close()
    conn.close()

    if not movies:
        raise HTTPException(status_code=404, detail="No movies found matching the query.")

    # Convert to list of dictionaries
    movie_list = []
    for movie in movies:
        movie_dict = {
            "id": movie[0],
            "title": movie[1],
            "original_title": movie[2],
            "type": movie[3],
            "start_year": movie[4],
            "runtime": movie[5],
            "genres": movie[6],
            "avg_rating": movie[7],
            "num_votes": movie[8],
        }
        movie_list.append(movie_dict)

    return movie_list
