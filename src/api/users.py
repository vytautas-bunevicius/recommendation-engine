# src/api/users.py

"""API module for handling user-related endpoints."""

from typing import Any, List, Optional, Dict

from fastapi import APIRouter, HTTPException, Path, Depends
from pydantic import BaseModel
from datetime import datetime
from src.database.connection import get_db_connection, close_db_connection
from psycopg2.extras import DictCursor
import logging  # Ensure logging is imported

router = APIRouter()

# Pydantic Models
class User(BaseModel):
    id: str
    name: str

class UserDetail(User):
    birth_year: Optional[int] = None

class ViewingHistoryItem(BaseModel):
    user_id: str
    movie_id: str
    watch_date: datetime
    watch_duration: int
    title: Optional[str] = None

class ViewingHistoryCreate(BaseModel):
    movie_id: str
    watch_date: datetime
    watch_duration: int

# Endpoints
@router.get("/", response_model=List[User])
def get_all_users() -> List[Dict[str, Any]]:
    """Retrieve all users."""
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor to get dict-like rows

    try:
        cursor.execute('SELECT id, name FROM users')
        users = cursor.fetchall()
    except Exception as e:
        logging.error(f"Error fetching users: {e}")
        raise HTTPException(status_code=500, detail="Error fetching users.")
    finally:
        cursor.close()
        close_db_connection(conn)  # Return connection to pool

    # Convert to list of dictionaries
    user_list = [{"id": user["id"], "name": user["name"]} for user in users]
    return user_list

@router.get("/{user_id}", response_model=UserDetail)
def get_user(user_id: str = Path(..., description="The UUID of the user")) -> Dict[str, Any]:
    """Retrieve user information."""
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor

    query = 'SELECT id, name, birth_year FROM users WHERE id = %s'
    try:
        cursor.execute(query, (user_id,))
        user = cursor.fetchone()
    except Exception as e:
        logging.error(f"Error fetching user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching user.")
    finally:
        cursor.close()
        close_db_connection(conn)  # Return connection to pool

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
    """Retrieve a user's viewing history."""
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor

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
        logging.error(f"Error fetching viewing history for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching viewing history.")
    finally:
        cursor.close()
        close_db_connection(conn)  # Return connection to pool

    if not history:
        raise HTTPException(status_code=404, detail="No viewing history found for this user.")

    # Convert to list of dictionaries
    history_list = []
    for item in history:
        history_dict = {
            "user_id": item["user_id"],
            "movie_id": item["movie_id"],
            "watch_date": item["watch_date"],
            "watch_duration": item["watch_duration"],
            "title": item["title"],
        }
        history_list.append(history_dict)

    return history_list

@router.post("/{user_id}/viewing_history", response_model=dict, status_code=201)
def add_viewing_history(
    user_id: str = Path(..., description="The UUID of the user"),
    viewing_history: ViewingHistoryCreate = ...,
) -> Dict[str, str]:
    """Add a viewing history entry for a user."""
    conn = get_db_connection()
    if conn is None:
        logging.error("Failed to connect to the database.")
        raise HTTPException(status_code=500, detail="Database connection failed.")
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor

    # Validate that the user exists
    user_query = "SELECT id FROM users WHERE id = %s"
    try:
        cursor.execute(user_query, (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"Error validating user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Error validating user.")

    # Validate that the movie exists
    movie_query = "SELECT id FROM movies WHERE id = %s"
    try:
        cursor.execute(movie_query, (viewing_history.movie_id,))
        movie = cursor.fetchone()
        if not movie:
            raise HTTPException(status_code=404, detail="Movie not found")
    except HTTPException as he:
        raise he
    except Exception as e:
        logging.error(f"Error validating movie {viewing_history.movie_id}: {e}")
        raise HTTPException(status_code=500, detail="Error validating movie.")

    # Insert viewing history
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
        logging.error(f"Error inserting viewing history for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Error inserting viewing history.")
    finally:
        cursor.close()
        close_db_connection(conn)  # Return connection to pool

    return {"message": "Viewing history added successfully"}
