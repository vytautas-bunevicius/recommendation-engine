# src/api/movies.py

import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from psycopg2.extras import DictCursor

from src.database.connection import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)

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
):
    """Retrieve a list of movies."""
    logger.info(f"Fetching movies with params: limit={limit}, offset={offset}, sort={sort}, order={order}, type={type}, min_votes={min_votes}")

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
        logger.error("Database connection failed")
        raise HTTPException(status_code=500, detail="Database connection failed.")

    cursor = conn.cursor(cursor_factory=DictCursor)

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
        logger.info(f"Found {len(movies)} movies")
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

    return [Movie(**movie) for movie in movies]

@router.get("/search", response_model=List[Movie])
def search_movies(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    type: str = Query("movie"),
    min_votes: int = Query(100, ge=0),
):
    """Search for movies by title using full-text search."""
    logger.info(f"Searching for movies with query: {query}, type: {type}, min_votes: {min_votes}")

    conn = get_db_connection()
    if conn is None:
        logger.error("Database connection failed")
        raise HTTPException(status_code=500, detail="Database connection failed.")

    cursor = conn.cursor(cursor_factory=DictCursor)

    # Create a tsquery from the search query
    tsquery = ' & '.join(query.split())

    sql = """
        SELECT m.id, m.title, m.original_title, m.type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes,
               ts_rank_cd(to_tsvector('english', m.title), to_tsquery(%s)) AS rank
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE to_tsvector('english', m.title) @@ to_tsquery(%s)
          AND m.type = %s
          AND COALESCE(mr.num_votes, 0) >= %s
        ORDER BY rank DESC, COALESCE(mr.num_votes, 0) DESC
        LIMIT %s OFFSET %s
    """
    try:
        cursor.execute(sql, (tsquery, tsquery, type, min_votes, limit, offset))
        movies = cursor.fetchall()
        logger.info(f"Found {len(movies)} movies matching the query")
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

    return [Movie(**movie) for movie in movies]

@router.get("/{movie_id}", response_model=Movie)
def get_movie(movie_id: str):
    """Retrieve details of a specific movie."""
    logger.info(f"Fetching movie with id: {movie_id}")

    conn = get_db_connection()
    if conn is None:
        logger.error("Database connection failed")
        raise HTTPException(status_code=500, detail="Database connection failed.")

    cursor = conn.cursor(cursor_factory=DictCursor)

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
        logger.error(f"Database query failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

    if movie:
        return Movie(**movie)
    else:
        logger.warning(f"Movie not found: {movie_id}")
        raise HTTPException(status_code=404, detail="Movie not found")
