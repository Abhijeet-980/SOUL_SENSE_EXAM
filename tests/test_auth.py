import pytest
import tempfile
import os
from app.auth import AuthManager
from app.db import get_connection

class TestAuth:
    def setup_method(self):
        # Create temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.auth_manager = AuthManager()
        
    def teardown_method(self):
        # Clean up temporary database
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
    
    def test_user_registration(self):
        # Test successful registration
        success, message = self.auth_manager.register_user("testuser", "password123")
        assert success == True
        assert "successful" in message
        
        # Test duplicate username
        success, message = self.auth_manager.register_user("testuser", "password456")
        assert success == False
        assert "already exists" in message
        
        # Test short username
        success, message = self.auth_manager.register_user("ab", "password123")
        assert success == False
        assert "at least 3 characters" in message
        
        # Test short password
        success, message = self.auth_manager.register_user("newuser", "123")
        assert success == False
        assert "at least 4 characters" in message
    
    def test_user_login(self):
        # Register a user first
        self.auth_manager.register_user("testuser", "password123")
        
        # Test successful login
        success, message = self.auth_manager.login_user("testuser", "password123")
        assert success == True
        assert "successful" in message
        assert self.auth_manager.current_user == "testuser"
        
        # Test wrong password
        success, message = self.auth_manager.login_user("testuser", "wrongpassword")
        assert success == False
        assert "Invalid" in message
        
        # Test non-existent user
        success, message = self.auth_manager.login_user("nonexistent", "password123")
        assert success == False
        assert "Invalid" in message
    
    def test_user_logout(self):
        # Register and login
        self.auth_manager.register_user("testuser", "password123")
        self.auth_manager.login_user("testuser", "password123")
        
        # Verify logged in
        assert self.auth_manager.is_logged_in() == True
        
        # Logout
        self.auth_manager.logout_user()
        
        # Verify logged out
        assert self.auth_manager.is_logged_in() == False
        assert self.auth_manager.current_user is None