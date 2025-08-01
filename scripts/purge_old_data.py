import os
import sys
import logging
import sqlite3
import datetime as dt

# --- Setup ---
# Add the project root to the Python path to allow imports from other directories
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Configure logging for the purge process
logging.basicConfig(level=logging.INFO, format='🧹 PURGE: %(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# --- Configuration ---
# The path to the database file, located in the parent directory.
DATABASE_PATH = os.path.join(project_root, 'miner_data.db')
# The retention period for data, in days. Data older than this will be deleted.
RETENTION_DAYS = 30

def purge_old_data():
    """
    Connects to the SQLite database and deletes records older than the retention period.
    """
    logging.info(f"Starting purge process for database: {DATABASE_PATH}")
    logging.info(f"Data retention period is set to {RETENTION_DAYS} days.")

    if not os.path.exists(DATABASE_PATH):
        logging.error(f"Database file not found at {DATABASE_PATH}. Aborting.")
        return

    try:
        # Calculate the cutoff datetime. Records created before this will be deleted.
        cutoff_date = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=RETENTION_DAYS)
        # SQLite stores datetimes as ISO 8601 strings.
        cutoff_date_str = cutoff_date.isoformat()

        logging.info(f"Calculated cutoff date: {cutoff_date_str}. Deleting all records older than this.")

        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.cursor()

            # First, count the number of records that will be deleted for logging purposes.
            cursor.execute("SELECT COUNT(*) FROM data_entities WHERE datetime < ?", (cutoff_date_str,))
            count_to_delete = cursor.fetchone()[0]

            if count_to_delete == 0:
                logging.info("No old records found to delete. Database is up to date.")
            else:
                logging.info(f"Found {count_to_delete} records to delete...")
                # Execute the DELETE statement.
                cursor.execute("DELETE FROM data_entities WHERE datetime < ?", (cutoff_date_str,))
                conn.commit()
                # --- CORRECTED: Replaced logging.success with logging.info ---
                logging.info(f"Successfully deleted {cursor.rowcount} old records from the database.")
            
            # Optional: Run VACUUM to reclaim disk space from the deleted rows.
            # This can be slow on very large databases.
            logging.info("Running VACUUM to optimize database file size...")
            conn.execute("VACUUM;")
            # --- CORRECTED: Replaced logging.success with logging.info ---
            logging.info("Database optimization complete.")

    except sqlite3.Error as e:
        logging.error(f"A database error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    purge_old_data()
