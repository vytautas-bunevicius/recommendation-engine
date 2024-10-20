"""API router for managing movies in the Movie Recommender system.

This module defines endpoints for retrieving, searching, and fetching details
of movies from the database. It utilizes FastAPI for handling HTTP requests
and Pydantic for data validation.
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from psycopg2.extras import DictCursor

from src.database.connection import get_db_connection

router = APIRouter()
logger = logging.getLogger(__name__)


class Movie(BaseModel):
    """Pydantic model representing a movie."""

    id: str
    title: str
    original_title: Optional[str]
    movie_type: str
    start_year: Optional[int]
    runtime: Optional[int]
    genres: Optional[str]
    avg_rating: Optional[float] = None
    num_votes: Optional[int] = None


@router.get("/", response_model=List[Movie])
def get_movies(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort: str = Query("popularity"),
    order: str = Query("desc"),
    movie_type: str = Query("movie"),
    min_votes: int = Query(1000, ge=0),
):
    """Retrieve a list of movies based on query parameters.

    Args:
        limit (int): The maximum number of movies to return. Defaults to 10.
        offset (int): The number of movies to skip before starting to collect the result set. Defaults to 0.
        sort (str): The field to sort the movies by. Options are 'popularity', 'rating', or 'year'. Defaults to 'popularity'.
        order (str): The order of sorting, either 'asc' for ascending or 'desc' for descending. Defaults to 'desc'.
        movie_type (str): The type of media to filter by, e.g., 'movie' or 'series'. Defaults to 'movie'.
        min_votes (int): The minimum number of votes a movie must have to be included. Defaults to 1000.

    Returns:
        List[Movie]: A list of movies matching the query parameters.

    Raises:
        HTTPException: If an invalid order parameter is provided or if the database connection/query fails.
    """
    logger.info(
        "Fetching movies with params: limit=%s, offset=%s, sort=%s, order=%s, movie_type=%s, min_votes=%s",
        limit,
        offset,
        sort,
        order,
        movie_type,
        min_votes,
    )

    sort_options = {
        "popularity": "mr.num_votes",
        "rating": "mr.avg_rating",
        "year": "m.start_year",
    }
    sort_column = sort_options.get(sort, "mr.num_votes")
    order_upper = order.upper()
    if order_upper not in ["ASC", "DESC"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid order parameter. Use 'asc' or 'desc'.",
        )

    conn = get_db_connection()
    if conn is None:
        logger.error("Database connection failed")
        raise HTTPException(status_code=500, detail="Database connection failed.")

    cursor = conn.cursor(cursor_factory=DictCursor)

    query = f"""
        SELECT m.id, m.title, m.original_title, m.type AS movie_type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.type = %s AND COALESCE(mr.num_votes, 0) >= %s
        ORDER BY {sort_column} {order_upper} NULLS LAST
        LIMIT %s OFFSET %s
    """
    try:
        cursor.execute(query, (movie_type, min_votes, limit, offset))
        movies = cursor.fetchall()
        logger.info("Found %s movies", len(movies))
    except Exception as e:
        logger.error("Database query failed: %s", e)
        raise HTTPException(
            status_code=500, detail="Database query failed."
        ) from e
    finally:
        cursor.close()
        conn.close()

    return [Movie(**movie) for movie in movies]


@router.get("/search", response_model=List[Movie])
def search_movies(
    query: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    movie_type: str = Query("movie"),
    min_votes: int = Query(100, ge=0),
):
    """Search for movies by title using full-text search.

    Args:
        query (str): The search query string. Must be at least 1 character long.
        limit (int): The maximum number of movies to return. Defaults to 10.
        offset (int): The number of movies to skip before starting to collect the result set. Defaults to 0.
        movie_type (str): The type of media to filter by, e.g., 'movie' or 'series'. Defaults to 'movie'.
        min_votes (int): The minimum number of votes a movie must have to be included. Defaults to 100.

    Returns:
        List[Movie]: A list of movies matching the search query.

    Raises:
        HTTPException: If the database connection/query fails.
    """
    logger.info(
        "Searching for movies with query: %s, movie_type: %s, min_votes: %s",
        query,
        movie_type,
        min_votes,
    )

    conn = get_db_connection()
    if conn is None:
        logger.error("Database connection failed")
        raise HTTPException(status_code=500, detail="Database connection failed.")

    cursor = conn.cursor(cursor_factory=DictCursor)

    tsquery = " & ".join(query.split())

    sql = """
        SELECT m.id, m.title, m.original_title, m.type AS movie_type, m.start_year,
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
        cursor.execute(sql, (tsquery, tsquery, movie_type, min_votes, limit, offset))
        movies = cursor.fetchall()
        logger.info("Found %s movies matching the query", len(movies))
    except Exception as e:
        logger.error("Database query failed: %s", e)
        raise HTTPException(
            status_code=500, detail="Database query failed."
        ) from e
    finally:
        cursor.close()
        conn.close()

    return [Movie(**movie) for movie in movies]


@router.get("/{movie_id}", response_model=Movie)
def get_movie(movie_id: str):
    """Retrieve details of a specific movie by its ID.

    Args:
        movie_id (str): The unique identifier of the movie.

    Returns:
        Movie: The movie details.

    Raises:
        HTTPException: If the movie is not found or if the database connection/query fails.
    """
    logger.info("Fetching movie with id: %s", movie_id)

    conn = get_db_connection()
    if conn is None:
        logger.error("Database connection failed")
        raise HTTPException(status_code=500, detail="Database connection failed.")

    cursor = conn.cursor(cursor_factory=DictCursor)

    query = """
        SELECT m.id, m.title, m.original_title, m.type AS movie_type, m.start_year,
               m.runtime, m.genres, mr.avg_rating, mr.num_votes
        FROM movies m
        LEFT JOIN movie_ratings mr ON m.id = mr.movie_id
        WHERE m.id = %s
    """
    try:
        cursor.execute(query, (movie_id,))
        movie = cursor.fetchone()
    except Exception as e:
        logger.error("Database query failed: %s", e)
        raise HTTPException(
            status_code=500, detail="Database query failed."
        ) from e
    finally:
        cursor.close()
        conn.close()

    if movie:
        return Movie(**movie)
    else:
        logger.warning("Movie not found: %s", movie_id)
        raise HTTPException(status_code=404, detail="Movie not found")
