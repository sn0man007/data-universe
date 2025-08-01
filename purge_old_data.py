import sqlite3
from datetime import datetime, timedelta
import os
import traceback

# --- CONFIGURATION ---
# IMPORTANT: Replace with the actual path to your miner's SQLite database file.
# This path is typically defined in your miner's configuration.
MINER_DB_PATH = 'miner_data.db' # Assumes the DB is in a 'data' subdirectory. Adjust as needed.
DAYS_TO_KEEP = 30

def purge_old_data(db_path: str, days_to_keep: int):
    """
    Connects to a SQLite database and deletes records from the 'data_entities'
    table that are older than a specified number of days.

    Args:
        db_path (str): The file path to the SQLite database.
        days_to_keep (int): The maximum age of records to keep, in days.
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        # The 'with' statement ensures the connection is closed automatically.
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Calculate the cutoff date. Any record with a timestamp before this will be deleted.
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)

            # The timestamp in the database is likely stored as a string or Unix timestamp.
            # This query handles the ISO 8601 string format 'YYYY-MM-DD HH:MM:SS'.
            # The 'datetime' column in the 'data_entities' table corresponds to the 'TimeBucket' concept.
            cutoff_iso_string = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

            table_name = 'data_entities'
            timestamp_column = 'datetime' # This is the typical column name for timestamped data.

            sql_count_query = f"SELECT COUNT(*) FROM {table_name}"
            sql_delete_query = f"DELETE FROM {table_name} WHERE {timestamp_column} <?"

            print(f"Connecting to database at {db_path}...")

            # Get record count before deleting for verification.
            cursor.execute(sql_count_query)
            initial_count = cursor.fetchone()
            print(f"Initial record count: {initial_count}")

            if initial_count == 0:
                print("Table is already empty. No action taken.")
                return

            print(f"Executing delete for records older than {days_to_keep} days (before {cutoff_iso_string})...")

            # Use a parameterized query to prevent SQL injection.
            cursor.execute(sql_delete_query, (cutoff_iso_string,))

            deleted_count = cursor.rowcount
            conn.commit()

            print(f"Successfully committed transaction. Deleted {deleted_count} records.")

            # Get record count after deleting for final verification.
            cursor.execute(sql_count_query)
            final_count = cursor.fetchone()
            print(f"Final record count: {final_count}")

    except sqlite3.Error as e:
        print(f"A database error occurred: {e}")
        print(traceback.format_exc())
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(traceback.format_exc())

if __name__ == '__main__':
    print("--- Starting Data Purge Operation ---")
    purge_old_data(MINER_DB_PATH, DAYS_TO_KEEP)
    print("--- Data Purge Operation Complete ---")
