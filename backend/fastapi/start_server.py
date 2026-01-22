"""
Quick start script for Soul Sense API server.
Handles installation and server startup.
"""
import subprocess
import sys
import os
from pathlib import Path

def check_dependencies():
    """Check if required packages are installed."""
    try:
        import fastapi
        import uvicorn
        import sqlalchemy
        return True
    except ImportError:
        return False

def install_dependencies():
    """Install required dependencies."""
    print("Installing dependencies...")
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ])
        print("✅ Dependencies installed successfully!")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        return False

def start_server(host="127.0.0.1", port=8000, reload=True):
    """Start the FastAPI server."""
    print(f"\n🚀 Starting Soul Sense API server...")
    print(f"   URL: http://{host}:{port}")
    print(f"   Docs: http://{host}:{port}/docs")
    print(f"   ReDoc: http://{host}:{port}/redoc")
    print("\nPress CTRL+C to stop the server\n")
    
    try:
        subprocess.run([
            sys.executable, "-m", "uvicorn",
            "app.main:app",
            "--host", host,
            "--port", str(port),
            "--reload" if reload else "--no-reload"
        ])
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped")

def main():
    """Main entry point."""
    print("="*60)
    print("    SOUL SENSE API - Quick Start")
    print("="*60)
    
    # Check dependencies
    if not check_dependencies():
        print("\n⚠️  Dependencies not found.")
        response = input("Install dependencies now? (y/n): ")
        
        if response.lower() == 'y':
            if not install_dependencies():
                sys.exit(1)
        else:
            print("Please install dependencies manually:")
            print("  pip install -r requirements.txt")
            sys.exit(1)
    else:
        print("\n✅ Dependencies OK")
    
    # Start server
    start_server()

if __name__ == "__main__":
    main()
