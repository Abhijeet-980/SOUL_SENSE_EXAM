import uvicorn
import argparse
import sys
import os
from pathlib import Path

# Add the project root and backend/fastapi to sys.path
# This ensures that 'api.main' and 'backend.core' imports work when bundled
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR))
sys.path.append(str(BASE_DIR.parent.parent))

from api.main import app

def main():
    parser = argparse.ArgumentParser(description="Soul Sense API Sidecar")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    args = parser.parse_args()

    print(f"Starting Soul Sense Sidecar on {args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")

if __name__ == "__main__":
    main()
