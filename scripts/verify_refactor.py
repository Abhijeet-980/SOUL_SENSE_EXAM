import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from app.main import SoulSenseApp
    print("SUCCESS: Imported SoulSenseApp")
except ImportError as e:
    print(f"FAILURE: Could not import SoulSenseApp: {e}")
    sys.exit(1)

# Mock root for init
try:
    import tkinter as tk
    root = tk.Tk()
    app = SoulSenseApp(root)
    print("SUCCESS: Instantiated SoulSenseApp (Baseline)")
    
    # Check for Refactor Attributes
    attributes = ['styles', 'auth', 'exam', 'results', 'settings_manager']
    missing = []
    for attr in attributes:
        if hasattr(app, attr):
            print(f"INFO: Found attribute '{attr}' - Refactor in progress/complete")
            # Verify specific method delegation
            if attr == 'results':
                if hasattr(app.results, 'show_visual_results'):
                     print("SUCCESS: ResultsManager has show_visual_results")
                else:
                     print("FAILURE: ResultsManager missing show_visual_results")
        else:
            missing.append(attr)
    
    print(f"INFO: Missing managers: {missing}")
    
    # Check new module locations
    try:
        from app.ml.predictor import SoulSenseMLPredictor
        print("SUCCESS: Imported app.ml.predictor")
    except ImportError:
        print("FAILURE: Could not import app.ml.predictor")
        
    try:
        from app.ui.dashboard import AnalyticsDashboard
        print("SUCCESS: Imported app.ui.dashboard")
    except ImportError:
        print("FAILURE: Could not import app.ui.dashboard")
        
    try:
        from app.ui.journal import JournalFeature
        print("SUCCESS: Imported app.ui.journal")
    except ImportError:
        print("FAILURE: Could not import app.ui.journal")

    root.destroy()
    sys.exit(0)
except Exception as e:
    print(f"FAILURE: Error during instantiation: {e}")
    sys.exit(1)
