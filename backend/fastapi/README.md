FastAPI scaffold

This folder contains a minimal FastAPI application scaffold.

Run locally:

1. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\\Scripts\\activate on Windows
pip install -r requirements.txt
```

2. Start the app:

```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Endpoints:
- GET /health
- GET /welcome
