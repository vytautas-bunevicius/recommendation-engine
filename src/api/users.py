"""API module for handling user-related endpoints.

This module provides endpoints for retrieving user information, managing viewing history,
and handling user-related operations in the Movie Recommender system.
"""

import logging
from datetime import datetime
from typing import Any, List, Optional, Dict

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel
from psycopg2.extras import DictCursor
from src.database.connection import get_db_connection, close_db_connection

router = APIRouter()


class User(BaseModel):
    """Pydantic model representing a user."""

    id: str
    name: str


class UserDetail(User):
    """Pydantic model representing detailed user information."""

    birth_year: Optional[int] = None


class ViewingHistoryItem(BaseModel):
    """Pydantic model representing an item in the user's viewing history."""

    user_id: str
    movie_id: str
    watch_date: datetime
    watch_duration: int
    title: Optional[str] = None


class ViewingHistoryCreate(BaseModel):
    """Pydantic model representing the data required to create a viewing history entry."""

    movie_id: str
    watch_date: datetime
    watch_duration: int


@router.get("/", response_model=List[User])
def get_all_users() -> List[Dict[str, Any]]:
    """Retrieve all users.

    Returns:
        List[Dict[str, Any]]: A list of users with their basic information.

    Raises:
        HTTPException: If there is an error connecting to the database or fetching users.
    """
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)

    try:
        cursor.execute('SELECT id, name FROM users')
        users = cursor.fetchall()
    except Exception as e:
        logging.error("Error fetching users: %s", e)
        raise HTTPException(status_code=500, detail="Error fetching users.") from e
    finally:
        cursor.close()
        close_db_connection(conn)

    user_list = [{"id": user["id"], "name": user["name"]} for user in users]
    return user_list


@router.get("/{user_id}", response_model=UserDetail)
def get_user(user_id: str = Path(..., description="The UUID of the user")) -> Dict[str, Any]:
    """Retrieve user information.

    Args:
        user_id (str): The unique identifier of the user.

    Returns:
        Dict[str, Any]: A dictionary containing the user's detailed information.

    Raises:
        HTTPException: If there is an error connecting to the database or fetching the user.
    """
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)

    query = 'SELECT id, name, birth_year FROM users WHERE id = %s'
    try:
        cursor.execute(query, (user_id,))
        user = cursor.fetchone()
    except Exception as e:
        logging.error("Error fetching user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Error fetching user.") from e
    finally:
        cursor.close()
        close_db_connection(conn)

    if user:
        user_dict = {
            "id": user["id"],
            "name": user["name"],
            "birth_year": user["birth_year"],
        }
        return user_dict
    else:
        raise HTTPException(status_code=404, detail="User not found")


@router.get("/{user_id}/viewing_history", response_model=List[ViewingHistoryItem])
def get_user_viewing_history(user_id: str = Path(..., description="The UUID of the user")) -> List[Dict[str, Any]]:
    """Retrieve a user's viewing history.

    Args:
        user_id (str): The unique identifier of the user.

    Returns:
        List[Dict[str, Any]]: A list of viewing history items for the user.

    Raises:
        HTTPException: If there is an error connecting to the database or fetching the viewing history.
    """
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)

    query = """
        SELECT vh.user_id, vh.movie_id, vh.watch_date, vh.watch_duration, m.title
        FROM viewing_history vh
        JOIN movies m ON vh.movie_id = m.id
        WHERE vh.user_id = %s
        ORDER BY vh.watch_date DESC
    """
    try:
        cursor.execute(query, (user_id,))
        history = cursor.fetchall()
    except Exception as e:
        logging.error("Error fetching viewing history for user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Error fetching viewing history.") from e
    finally:
        cursor.close()
        close_db_connection(conn)

    if not history:
        raise HTTPException(status_code=404, detail="No viewing history found for this user.")

    history_list = [
        {
            "user_id": item["user_id"],
            "movie_id": item["movie_id"],
            "watch_date": item["watch_date"],
            "watch_duration": item["watch_duration"],
            "title": item["title"],
        }
        for item in history
    ]

    return history_list


@router.post("/{user_id}/viewing_history", response_model=dict, status_code=201)
def add_viewing_history(
    user_id: str = Path(..., description="The UUID of the user"),
    viewing_history: ViewingHistoryCreate = ...,
) -> Dict[str, str]:
    """Add a viewing history entry for a user.

    Args:
        user_id (str): The unique identifier of the user.
        viewing_history (ViewingHistoryCreate): The viewing history data to be added.

    Returns:
        Dict[str, str]: A message indicating the success of the operation.

    Raises:
        HTTPException: If there is an error connecting to the database, validating the user or movie, or inserting the viewing history.
    """
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)

    user_query = "SELECT id FROM users WHERE id = %s"
    try:
        cursor.execute(user_query, (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error("Error validating user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Error validating user.") from e

    movie_query = "SELECT id FROM movies WHERE id = %s"
    try:
        cursor.execute(movie_query, (viewing_history.movie_id,))
        movie = cursor.fetchone()
        if not movie:
            raise HTTPException(status_code=404, detail="Movie not found")
    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error("Error validating movie %s: %s", viewing_history.movie_id, e)
        raise HTTPException(status_code=500, detail="Error validating movie.") from e

    insert_query = """
        INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration)
        VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(
            insert_query,
            (user_id, viewing_history.movie_id, viewing_history.watch_date, viewing_history.watch_duration),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error("Error inserting viewing history for user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Error inserting viewing history.") from e
    finally:
        cursor.close()
        close_db_connection(conn)

    return {"message": "Viewing history added successfully"}
