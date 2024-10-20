from typing import Any, List, Dict
from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel
from datetime import datetime
from src.database.connection import get_db_connection
from psycopg2.extras import DictCursor  # Import DictCursor

router = APIRouter()

# Pydantic Models
class User(BaseModel):
    id: str
    name: str

class UserDetail(User):
    birth_year: int = None  # Assuming 'birth_year' is a field in the 'users' table

class ViewingHistoryItem(BaseModel):
    user_id: str
    movie_id: str
    watch_date: datetime
    watch_duration: int
    title: str = None  # From JOIN with movies

class ViewingHistoryCreate(BaseModel):
    movie_id: str
    watch_date: datetime
    watch_duration: int

# Endpoints
@router.get("/", response_model=List[User])
def get_all_users() -> List[Dict[str, Any]]:
    """Retrieve all users."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor

    try:
        cursor.execute('SELECT id, name FROM users')
        users = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

    print(f"Retrieved {len(users)} users from database")
    for user in users:
        print(f"User: {dict(user)}")  # Convert to dict if necessary

    return [dict(user) for user in users]  # Ensure response is list of dicts

@router.get("/{user_id}", response_model=UserDetail)
def get_user(user_id: str = Path(..., description="The UUID of the user")) -> Dict[str, Any]:
    """Retrieve user information."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor

    try:
        cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        user = cursor.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

    if user:
        return dict(user)  # Convert to dict
    raise HTTPException(status_code=404, detail="User not found")

@router.get("/{user_id}/viewing_history", response_model=List[ViewingHistoryItem])
def get_user_viewing_history(user_id: str = Path(..., description="The UUID of the user")) -> List[Dict[str, Any]]:
    """Retrieve a user's viewing history."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)  # Use DictCursor

    try:
        cursor.execute(
            """
            SELECT vh.user_id, vh.movie_id, vh.watch_date, vh.watch_duration, m.title
            FROM viewing_history vh
            JOIN movies m ON vh.movie_id = m.id
            WHERE vh.user_id = %s
            ORDER BY vh.watch_date DESC
            """,
            (user_id,),
        )
        history = cursor.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

    return [dict(item) for item in history]

@router.post("/{user_id}/viewing_history", response_model=dict, status_code=201)
def add_viewing_history(
    user_id: str = Path(..., description="The UUID of the user"),
    viewing_history: ViewingHistoryCreate = ...,
) -> Dict[str, str]:
    """Add a viewing history entry for a user."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO viewing_history (user_id, movie_id, watch_date, watch_duration)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, viewing_history.movie_id, viewing_history.watch_date, viewing_history.watch_duration),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()

    return {"message": "Viewing history added successfully"}
