import sys
import os
import logging
import sqlite3 # Make sure sqlite3 is imported

# Add the project root to the Python path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Import only what is absolutely necessary
from storage.miner.sqlite_miner_storage import SqliteMinerStorage

# DEFINE THE DATABASE NAME LOCALLY to prevent side-effects
DATABASE_NAME = "miner_data.db"

def setup_database():
    """A clean, dedicated script to create and set up the database."""
    logging.basicConfig(level=logging.INFO, format='🛠️ SETUP: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.info(f"--- Running Database Setup for '{DATABASE_NAME}' ---")
    
    if os.path.exists(DATABASE_NAME):
        logging.warning(f"Removing existing database file: {DATABASE_NAME}")
        os.remove(DATABASE_NAME)

    # This will now work because no conflicting libraries are active.
    storage = SqliteMinerStorage(database=DATABASE_NAME)
    
    logging.info("✅ Database and tables created successfully.")
    logging.info("--- Setup Complete ---")

if __name__ == "__main__":
    setup_database()
