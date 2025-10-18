#----------------------------------------------------
# Script Name: connection.py
# Description: Connects the NYC Train Mobility database
# Author:      Santhiana Ange Kaze
# Date:        2025-10-14
#-----------------------------------------------------

import sqlite3
import os
from typing import Optional

class DatabaseConnection:
    # DB connection handler with folder management and script execution

    def __init__(self, db_path: str = "nyc_train.db"):
        self.db_path = db_path
        self._ensure_db_directory()

    def _ensure_db_directory(self):
        # Ensure DB folder exists
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

    def get_connection(self) -> sqlite3.Connection:
        # Get a SQLite DB connection that uses Row factory for dict-like row access.
        # Caller should use context management with this connection.
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def execute_script(self, script: str) -> bool:
        # Execute a SQL script
        try:
            with self.get_connection() as conn:
                conn.executescript(script)
                return True
        except Exception as e:
            print(f"Error executing script: {e}")
            return False

# Global DB connection instance
_db_connection = DatabaseConnection()

def get_connection() -> sqlite3.Connection:
    # Get DB connection
    return _db_connection.get_connection()

def set_database_path(path: str):
    # Set DB path
    global _db_connection
    _db_connection = DatabaseConnection(path)
  
