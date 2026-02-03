
import sqlite3
import os
import sys

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "soulsense.db")

print(f"Target DB: {DB_PATH}")

def patch_db():
    if not os.path.exists(DB_PATH):
        print("Database not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        print("Starting PR 1 Manual Patch...")
        
        # 1. Add columns to users table
        print("Checking 'users' columns...")
        cursor.execute("PRAGMA table_info(users)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if "is_active" not in columns:
            print("Adding is_active...")
            cursor.execute("ALTER TABLE users ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT 1")
            
        if "otp_secret" not in columns:
            print("Adding otp_secret...")
            cursor.execute("ALTER TABLE users ADD COLUMN otp_secret VARCHAR")
            
        if "is_2fa_enabled" not in columns:
            print("Adding is_2fa_enabled...")
            cursor.execute("ALTER TABLE users ADD COLUMN is_2fa_enabled BOOLEAN NOT NULL DEFAULT 0")
            
        if "last_activity" not in columns:
            print("Adding last_activity...")
            cursor.execute("ALTER TABLE users ADD COLUMN last_activity VARCHAR")

        # 2. Add columns to login_attempts
        print("Checking 'login_attempts' columns...")
        cursor.execute("PRAGMA table_info(login_attempts)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if "user_agent" not in columns:
            print("Adding user_agent...")
            cursor.execute("ALTER TABLE login_attempts ADD COLUMN user_agent VARCHAR")
            
        if "failure_reason" not in columns:
            print("Adding failure_reason...")
            cursor.execute("ALTER TABLE login_attempts ADD COLUMN failure_reason VARCHAR")

        # 3. Create otp_codes table
        print("Checking 'otp_codes' table...")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='otp_codes'")
        if not cursor.fetchone():
            print("Creating otp_codes table...")
            cursor.execute("""
                CREATE TABLE otp_codes (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    code_hash VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    created_at DATETIME,
                    expires_at DATETIME NOT NULL,
                    is_used BOOLEAN,
                    attempts INTEGER,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
            """)
            cursor.execute("CREATE INDEX ix_otp_codes_user_id ON otp_codes (user_id)")
        
        conn.commit()
        print("Patch applied successfully!")
        
    except Exception as e:
        print(f"Error patching DB: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    patch_db()
