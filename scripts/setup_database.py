# setup_database.py (Corrected Version)
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "miner_data.db")

def create_database():
    """Creates the SQLite database and the data_entities table with the corrected schema."""
    print(f"Setting up database at: {DB_PATH}")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # ✅ FIX: Renamed the main content column from 'content' to 'text'
        # to align with the standard Subnet 13 DataEntity schema.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uri TEXT UNIQUE NOT NULL,
                datetime TEXT NOT NULL,
                source TEXT NOT NULL,
                label TEXT NOT NULL,
                text TEXT
            )
        ''')
        
        print("Table 'data_entities' created or already exists with the correct 'text' column.")
        print("Database setup complete.")

if __name__ == "__main__":
    create_database()
