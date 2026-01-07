import hashlib
import sqlite3
from datetime import datetime
from app.db import get_connection

class AuthManager:
    def __init__(self):
        self.current_user = None
        self.ensure_users_table()
    
    def ensure_users_table(self):
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_login TEXT
        )
        """)
        conn.commit()
        conn.close()
    
    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()
    
    def register_user(self, username, password):
        if len(username) < 3:
            return False, "Username must be at least 3 characters"
        if len(password) < 4:
            return False, "Password must be at least 4 characters"
        
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT username FROM users WHERE username = ?", (username,))
            if cursor.fetchone():
                return False, "Username already exists"
            
            password_hash = self.hash_password(password)
            created_at = datetime.utcnow().isoformat()
            
            cursor.execute("""
            INSERT INTO users (username, password_hash, created_at)
            VALUES (?, ?, ?)
            """, (username, password_hash, created_at))
            
            conn.commit()
            return True, "Registration successful"
        
        except sqlite3.Error as e:
            return False, f"Registration failed: {str(e)}"
        finally:
            conn.close()
    
    def login_user(self, username, password):
        conn = get_connection()
        cursor = conn.cursor()
        
        try:
            password_hash = self.hash_password(password)
            cursor.execute("""
            SELECT username FROM users 
            WHERE username = ? AND password_hash = ?
            """, (username, password_hash))
            
            user = cursor.fetchone()
            if user:
                cursor.execute("""
                UPDATE users SET last_login = ? WHERE username = ?
                """, (datetime.utcnow().isoformat(), username))
                conn.commit()
                
                self.current_user = username
                return True, "Login successful"
            else:
                return False, "Invalid username or password"
        
        except sqlite3.Error as e:
            return False, f"Login failed: {str(e)}"
        finally:
            conn.close()
    
    def logout_user(self):
        self.current_user = None
    
    def is_logged_in(self):
        return self.current_user is not None