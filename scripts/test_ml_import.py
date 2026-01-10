import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

print("Testing app.ui.dashboard import...")
try:
    from app.ui.dashboard import AnalyticsDashboard
    print("SUCCESS: Imported AnalyticsDashboard")
except ImportError as e:
    print(f"FAILURE (ImportError): {e}")
except Exception as e:
    print(f"FAILURE (Exception): {e}")

print("\nTesting app.ui.journal import...")
try:
    from app.ui.journal import JournalFeature
    print("SUCCESS: Imported JournalFeature")
except ImportError as e:
    print(f"FAILURE (ImportError): {e}")
except Exception as e:
    print(f"FAILURE (Exception): {e}")
