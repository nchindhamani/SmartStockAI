# data/users_store.py
# PostgreSQL store for application users (email + password hash)

from typing import Optional
from uuid import uuid4

from data.db_connection import get_connection


class UsersStore:
    """Create and look up users. Passwords are stored as bcrypt hashes only."""

    def __init__(self):
        self._init_table()

    def _init_table(self):
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    password_hash VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_lower ON users (LOWER(email))"
            )

    def get_by_email(self, email: str) -> Optional[dict]:
        normalized = email.strip().lower()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, email, password_hash, created_at
                FROM users
                WHERE LOWER(email) = %s
                """,
                (normalized,),
            )
            row = cursor.fetchone()
        return self._row_to_dict(row)

    def get_by_id(self, user_id: str) -> Optional[dict]:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, email, password_hash, created_at
                FROM users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = cursor.fetchone()
        return self._row_to_dict(row)

    def create(self, email: str, password_hash: str) -> dict:
        user_id = str(uuid4())
        normalized = email.strip().lower()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO users (id, email, password_hash)
                VALUES (%s, %s, %s)
                RETURNING id, email, password_hash, created_at
                """,
                (user_id, normalized, password_hash),
            )
            row = cursor.fetchone()
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> Optional[dict]:
        if row is None:
            return None
        if isinstance(row, dict):
            created = row.get("created_at")
            return {
                "id": str(row["id"]),
                "email": row["email"],
                "password_hash": row["password_hash"],
                "created_at": created.isoformat() if created else None,
            }
        created = row[3]
        return {
            "id": str(row[0]),
            "email": row[1],
            "password_hash": row[2],
            "created_at": created.isoformat() if created else None,
        }


_users_store: Optional[UsersStore] = None


def get_users_store() -> UsersStore:
    global _users_store
    if _users_store is None:
        _users_store = UsersStore()
    return _users_store
