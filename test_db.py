import sqlite3
import os

DB_FILE = "test.db"

# Ensure we're working with a clean slate
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

print(f"Attempting to create database at: {os.path.abspath(DB_FILE)}")

try:
    # Connect to the database (this will create the file)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Create a simple table
    cursor.execute("""
    CREATE TABLE canary (
        id INTEGER PRIMARY KEY,
        message TEXT NOT NULL
    );
    """)

    # Insert a single row
    cursor.execute("INSERT INTO canary (message) VALUES (?)", ("Success!",))

    # Commit the changes to disk
    conn.commit()

    # Close the connection
    conn.close()
    
    print("✅ Script finished successfully.")

except Exception as e:
    print(f"❌ An error occurred: {e}")
