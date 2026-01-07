import logging

def ensure_scores_schema(cursor):
    """
    Ensure scores table has required columns.
    Adds 'age' column if missing for backward compatibility.
    Adds 'detailed_age_group' column if missing for enhanced analytics.
    """
    cursor.execute("PRAGMA table_info(scores)")
    cols = [c[1] for c in cursor.fetchall()]
    
    if cols and "age" not in cols:
        logging.info("Migrating scores table: adding age column")
        cursor.execute("ALTER TABLE scores ADD COLUMN age INTEGER")
    
    if cols and "detailed_age_group" not in cols:
        logging.info("Migrating scores table: adding detailed_age_group column")
        cursor.execute("ALTER TABLE scores ADD COLUMN detailed_age_group TEXT")
    
    # Add user_id column for linking scores to authenticated users
    if cols and "user_id" not in cols:
        logging.info("Migrating scores table: adding user_id column")
        cursor.execute("ALTER TABLE scores ADD COLUMN user_id INTEGER")


def ensure_responses_schema(cursor):
    """
    Ensure responses table has required columns.
    Maintains backward-compatible 'age_group' column.
    Adds 'detailed_age_group' column if missing for enhanced analytics.
    """
    cursor.execute("PRAGMA table_info(responses)")
    cols = [c[1] for c in cursor.fetchall()]

    if not cols:
        cursor.execute("""
        CREATE TABLE responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            question_id INTEGER,
            response_value INTEGER,
            age_group TEXT,
            detailed_age_group TEXT,
            timestamp TEXT
        )
        """)
    else:
        required = {
            "username": "TEXT",
            "question_id": "INTEGER",
            "response_value": "INTEGER",
            "age_group": "TEXT",
            "timestamp": "TEXT"
        }
        for col, t in required.items():
            if col not in cols:
                cursor.execute(f"ALTER TABLE responses ADD COLUMN {col} {t}")
        
        # Add detailed_age_group for enhanced analytics
        if "detailed_age_group" not in cols:
            logging.info("Migrating responses table: adding detailed_age_group column")
            cursor.execute("ALTER TABLE responses ADD COLUMN detailed_age_group TEXT")

    # Add user_id column for linking responses to authenticated users
    cursor.execute("PRAGMA table_info(responses)")
    cols = [c[1] for c in cursor.fetchall()]
    if "user_id" not in cols:
        logging.info("Migrating responses table: adding user_id column")
        cursor.execute("ALTER TABLE responses ADD COLUMN user_id INTEGER")


def ensure_question_bank_schema(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS question_bank (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_text TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT
    )
    """)

def ensure_users_schema(cursor):
    """
    Ensure users table exists for authentication.
    """
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        last_login TEXT
    )
    """)
