# 📁 Project Structure — SOUL_SENSE_EXAM

This document explains the folder structure of the **SOUL_SENSE_EXAM** repository and the purpose of its key files and directories. The project is organized to promote modularity, maintainability, and ease of collaboration.

---

```text
SOUL_SENSE_EXAM/
│
├── app/                     # Core application package
│   ├── __init__.py
│   ├── main.py              # Tkinter application entry point
│   ├── db.py                # Database connection utilities
│   ├── models.py            # Schema & migrations
│   ├── questions.py         # Question loading from DB
│   └── utils.py             # Shared helpers (e.g., age grouping)
│
├── scripts/                 # One-time / maintenance scripts
│   ├── __init__.py
│   └── load_questions.py    # Loads questions.txt into the DB (run once)
│
├── data/
│   └── questions.txt        # Source question bank (seed data)
│
├── db/
│   └── soulsense.db         # SQLite database (generated at runtime)
│
├── tests/                   # Pytest test suite
│   ├── test_db.py
│   ├── test_models.py
│   ├── test_questions.py
│   └── test_utils.py
│
├── logs/
│   └── soulsense.log        # Application logs
│
├── pytest.ini               # Pytest configuration
└── README.md
```

---

## Directory & File Overview

###  `app/`

The main application package containing all core logic.

* **`__init__.py`** – Marks the directory as a Python package.
* **`main.py`** – Entry point for the Tkinter GUI application.
* **`db.py`** – Handles SQLite database connections and queries.
* **`models.py`** – Defines database schema and model-level operations.
* **`questions.py`** – Fetches and manages questions from the database.
* **`utils.py`** – Common helper functions shared across the app (e.g., age grouping, validations).

---

###  `scripts/`

Contains utility scripts meant to be run manually or once.

* **`load_questions.py`** – Reads questions from `data/questions.txt` and seeds them into the database.

---

###  `data/`

Stores raw/static data used by the application.

* **`questions.txt`** – Original question bank used to populate the database.

---

###  `db/`

Holds database-related files.

* **`soulsense.db`** – SQLite database file generated during runtime and used to store questions and user responses.

---

###  `tests/`

Automated test suite using **pytest** to ensure code reliability.

* **`test_db.py`** – Tests database connectivity and operations.
* **`test_models.py`** – Tests database models and schema logic.
* **`test_questions.py`** – Tests question loading and retrieval.
* **`test_utils.py`** – Tests shared utility functions.

---

###  `logs/`

Stores runtime logs for debugging and monitoring.

* **`soulsense.log`** – Application log file capturing errors and events.

---

###  Root-Level Files

* **`pytest.ini`** – Configuration file for pytest.
* **`README.md`** – High-level project overview, setup instructions, and usage guide.

---

## Benefits of This Structure

* Clear separation of concerns
* Easy testing and debugging
* Scalable for future features
* Contributor-friendly and maintainable

---

*This structure is designed to support a clean, modular emotional intelligence assessment application built with Python and Tkinter.*
