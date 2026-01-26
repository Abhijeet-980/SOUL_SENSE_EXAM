# Refactoring Monolithic main.py

## Overview
Refactor the monolithic `app/main.py` file (over 500 lines) to separate concerns: UI initialization, authentication, view switching, database operations, and shutdown logic.

## Steps to Complete

### 1. Create app/ui/app_initializer.py ✅
- Extract UI setup logic from SoulSenseApp.__init__ (window title, geometry, styles, fonts, etc.)
- Move login screen display logic to this module
- Handle theme application and UI layout initialization

### 2. Create app/ui/view_manager.py ✅
- Extract view switching logic from switch_view method
- Include methods like show_home, show_dashboard, show_journal, etc.
- Handle content area clearing and view rendering

### 3. Create app/auth/app_auth.py
- Move authentication-related methods from SoulSenseApp
- Include login screen, user settings loading, post-login initialization
- Integrate with existing AuthManager in app/auth.py

### 4. Create app/shutdown_handler.py ✅
- Extract graceful shutdown logic from graceful_shutdown method
- Handle signal handlers, atexit registration, and database session cleanup

### 5. Refactor app/main.py ✅
- Simplify SoulSenseApp class to coordinate the new modules
- Remove extracted code and delegate to the new classes
- Ensure proper initialization and integration

### 6. Update Imports and Dependencies ✅
- Adjust imports in main.py for the new modules
- Test that all functionality works after refactoring

### 7. Testing and Validation ✅
- Run the application to ensure no regressions
- Verify login, view switching, and shutdown work correctly
