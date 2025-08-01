# verify_db.py
import sqlite3
import os

# Define the path for the database, ensuring it points to the correct location
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "miner_data.db")

def verify_database_contents():
    """
    Connects to the SQLite database and prints verification info.
    """
    print(f"Verifying database at: {DB_PATH}\n")

    if not os.path.exists(DB_PATH):
        print("Database file not found. Please run the etl_pipeline.py script first.")
        return

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()

            # 1. Count the total number of records
            cursor.execute("SELECT COUNT(*) FROM data_entities")
            total_rows = cursor.fetchone()[0]
            print(f"✅ Total records in data_entities table: {total_rows}\n")

            # 2. Count records by source to provide a clear breakdown
            print("Breakdown by source:")
            cursor.execute("SELECT source, COUNT(*) FROM data_entities GROUP BY source")
            source_counts = cursor.fetchall()
            for source, count in source_counts:
                print(f"  - {source}: {count} records")

            # 3. Fetch and display a sample of 5 records
            print("\nSample of 5 records:")
            cursor.execute("SELECT id, uri, source, label, datetime FROM data_entities LIMIT 5")
            sample_rows = cursor.fetchall()

            if not sample_rows:
                print("  - No records found in the database.")
            else:
                for row in sample_rows:
                    print("-" * 20)
                    print(f"  ID: {row[0]}")
                    print(f"  URI: {row[1]}")
                    print(f"  Source: {row[2]}")
                    print(f"  Label: {row[3]}")
                    print(f"  Datetime: {row[4]}")
                print("-" * 20)

    except sqlite3.Error as e:
        print(f"Database error: {e}")

if __name__ == "__main__":
    verify_database_contents()
