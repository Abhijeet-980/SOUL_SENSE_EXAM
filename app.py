import sqlite3
import tkinter as tk
from tkinter import messagebox
import time
import winsound

# ========== SETTINGS FEATURE ADDED HERE ==========
# Global settings
CURRENT_THEME = "light"
QUESTION_COUNT = 5
SOUND_ENABLED = True

# Theme colors
THEMES = {
    "light": {
        "bg": "#f0f4f8",
        "text": "#333333",
        "primary": "#4CAF50",
        "warning": "#E53935",
        "secondary": "#2196F3",
        "accent": "#FF9800",
        "header_bg": "#1E1E2F",
        "header_fg": "white",
        "timer": "#1E88E5"
    },
    "dark": {
        "bg": "#121212",
        "text": "#ffffff",
        "primary": "#4CAF50",
        "warning": "#E53935",
        "secondary": "#2196F3",
        "accent": "#FF9800",
        "header_bg": "#0a0a15",
        "header_fg": "white",
        "timer": "#64B5F6"
    }
}

def get_theme():
    return THEMES[CURRENT_THEME]

def apply_theme_to_window(window):
    """Apply current theme to a window and all its widgets"""
    theme = get_theme()
    window.configure(bg=theme["bg"])
    
    # Apply to all widgets in window
    for widget in window.winfo_children():
        widget_type = widget.winfo_class()
        try:
            if widget_type == "Label":
                if widget.cget("bg") in ["#1E1E2F", "#0a0a15"]:
                    widget.configure(bg=theme["header_bg"], fg=theme["header_fg"])
                elif widget.cget("fg") == "#1E88E5":
                    widget.configure(bg=theme["bg"], fg=theme["timer"])
                else:
                    widget.configure(bg=theme["bg"], fg=theme["text"])
            elif widget_type == "Button":
                current_bg = widget.cget("bg")
                if current_bg == "#4CAF50":
                    widget.configure(bg=theme["primary"], fg="white")
                elif current_bg == "#E53935":
                    widget.configure(bg=theme["warning"], fg="white")
                elif current_bg == "#2196F3":
                    widget.configure(bg=theme["secondary"], fg="white")
                elif current_bg == "#FF9800":
                    widget.configure(bg=theme["accent"], fg="white")
                elif current_bg == "#757575":
                    widget.configure(bg="#757575", fg="white")
            elif widget_type == "Entry":
                widget.configure(bg="white" if CURRENT_THEME == "light" else "#2d2d2d", 
                               fg=theme["text"])
            elif widget_type == "Radiobutton":
                widget.configure(bg=theme["bg"], fg=theme["text"])
            elif widget_type == "Scale":
                widget.configure(bg=theme["bg"], fg=theme["text"])
        except:
            pass

def play_sound(sound_type):
    if not SOUND_ENABLED:
        return
    try:
        if sound_type == "click": winsound.Beep(1000, 100)
        elif sound_type == "success": winsound.Beep(1500, 200)
        elif sound_type == "error": winsound.Beep(800, 300)
    except: 
        pass

def open_settings(parent_window):
    settings = tk.Toplevel(parent_window)
    settings.title("Settings")
    settings.geometry("400x450")
    settings.resizable(False, False)
    
    theme = get_theme()
    settings.configure(bg=theme["bg"])
    
    tk.Label(settings, text="⚙️ Settings", font=("Arial", 22, "bold"), 
             bg=theme["bg"], fg=theme["text"]).pack(pady=20)
    
    # Theme toggle
    tk.Label(settings, text="Theme:", font=("Arial", 12), 
             bg=theme["bg"], fg=theme["text"]).pack()
    
    def toggle_theme():
        global CURRENT_THEME
        CURRENT_THEME = "dark" if CURRENT_THEME == "light" else "light"
        play_sound("click")
        
        # Apply theme to settings window
        new_theme = get_theme()
        settings.configure(bg=new_theme["bg"])
        for widget in settings.winfo_children():
            try:
                if widget.winfo_class() == "Label":
                    widget.configure(bg=new_theme["bg"], fg=new_theme["text"])
                elif widget.winfo_class() == "Button":
                    current_bg = widget.cget("bg")
                    if current_bg == "#4CAF50":
                        widget.configure(bg=new_theme["primary"])
                    elif current_bg == "#2196F3":
                        widget.configure(bg=new_theme["secondary"])
                    elif current_bg == "#757575":
                        widget.configure(bg="#757575")
                elif widget.winfo_class() == "Scale":
                    widget.configure(bg=new_theme["bg"], fg=new_theme["text"])
            except:
                pass
        
        # Apply theme to parent window
        apply_theme_to_window(parent_window)
        
        messagebox.showinfo("Theme Changed", 
                          f"{CURRENT_THEME.capitalize()} theme applied successfully!")
    
    tk.Button(settings, text="☀️ Toggle Light/Dark", command=toggle_theme, 
              bg=theme["primary"], fg="white", font=("Arial", 11), 
              width=20).pack(pady=5)
    
    # Question count
    tk.Label(settings, text="Question Count:", font=("Arial", 12), 
             bg=theme["bg"], fg=theme["text"]).pack(pady=(10,0))
    
    count_value = tk.StringVar(value=str(QUESTION_COUNT))
    
    def update_count(val):
        global QUESTION_COUNT
        QUESTION_COUNT = int(float(val))
        count_value.set(str(QUESTION_COUNT))
        play_sound("click")
    
    scale = tk.Scale(settings, from_=3, to=5, orient="horizontal", 
                    command=update_count, bg=theme["bg"], fg=theme["text"], 
                    length=150)
    scale.set(QUESTION_COUNT)
    scale.pack()
    
    tk.Label(settings, textvariable=count_value, font=("Arial", 12, "bold"), 
             bg=theme["bg"], fg=theme["primary"]).pack()
    
    # Sound toggle
    tk.Label(settings, text="Sound:", font=("Arial", 12), 
             bg=theme["bg"], fg=theme["text"]).pack(pady=(10,0))
    
    def toggle_sound():
        global SOUND_ENABLED
        SOUND_ENABLED = not SOUND_ENABLED
        play_sound("click")
        status = "enabled" if SOUND_ENABLED else "disabled"
        messagebox.showinfo("Sound", f"Sound effects {status}!")
    
    tk.Button(settings, text="🔊 Toggle Sound", command=toggle_sound, 
              bg=theme["secondary"], fg="white", font=("Arial", 11), 
              width=20).pack(pady=5)
    
    tk.Button(settings, text="Close", command=settings.destroy, 
              bg="#757575", fg="white", font=("Arial", 12), 
              width=15).pack(pady=20)
# ========== END OF SETTINGS FEATURE ==========

#RETRY MECHANISM
def retry_operation(operation, retries=3, delay=0.5, backoff=2):
    attempt = 0
    while attempt < retries:
        try:
            return operation()
        except (sqlite3.OperationalError, IOError):
            attempt += 1
            if attempt == retries:
                raise
            time.sleep(delay)
            delay *= backoff

#DATABASE SETUP
conn = sqlite3.connect("soulsense_db.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    age INTEGER,
    total_score INTEGER,

    avg_response REAL,
    max_response INTEGER,
    min_response INTEGER,
    score_variance REAL,

    questions_attempted INTEGER,
    completion_ratio REAL,
    avg_time_per_question REAL,
    time_taken_seconds INTEGER
)
""")
conn.commit()

#QUESTIONS
questions = [
    {"text": "You can recognize your emotions as they happen.", "age_min": 12, "age_max": 25},
    {"text": "You find it easy to understand why you feel a certain way.", "age_min": 14, "age_max": 30},
    {"text": "You can control your emotions even in stressful situations.", "age_min": 15, "age_max": 35},
    {"text": "You reflect on your emotional reactions to situations.", "age_min": 13, "age_max": 28},
    {"text": "You are aware of how your emotions affect others.", "age_min": 16, "age_max": 40}
]

#ANALYTICS
def compute_analytics(responses, time_taken, total_questions):
    n = len(responses)
    if n == 0:
        return {
            "avg": 0, "max": 0, "min": 0, "variance": 0,
            "attempted": 0, "completion": 0, "avg_time": 0
        }

    avg = sum(responses) / n
    variance = sum((x - avg) ** 2 for x in responses) / n

    return {
        "avg": round(avg, 2),
        "max": max(responses),
        "min": min(responses),
        "variance": round(variance, 2),
        "attempted": n,
        "completion": round(n / total_questions, 2),
        "avg_time": round(time_taken / n, 2)
    }

#SPLASH SCREEN
def show_splash():
    splash = tk.Tk()
    splash.title("SoulSense")
    splash.geometry("500x300")
    splash.resizable(False, False)
    
    theme = get_theme()
    splash.configure(bg=theme["header_bg"])

    tk.Label(
        splash, text="SoulSense",
        font=("Arial", 32, "bold"),
        fg="white", bg=theme["header_bg"]
    ).pack(pady=40)

    tk.Label(
        splash, text="Emotional Awareness Assessment",
        font=("Arial", 14),
        fg="#CCCCCC", bg=theme["header_bg"]
    ).pack()

    tk.Label(
        splash,
        text="Loading...",
        font=("Arial", 15),
        fg="white",
        bg=theme["header_bg"]
    ).pack(pady=30)
    splash.after(2500, lambda: (play_sound("success"), splash.destroy(), show_user_details()))
    splash.mainloop()

#USER DETAILS
def show_user_details():
    root = tk.Tk()
    root.title("SoulSense - User Details")
    root.geometry("450x380")
    root.resizable(False, False)
    
    theme = get_theme()
    root.configure(bg=theme["bg"])

    username = tk.StringVar()
    age = tk.StringVar()

    tk.Label(root, text="SoulSense Assessment",
             font=("Arial", 20, "bold"),
             bg=theme["bg"], fg=theme["text"]).pack(pady=20)

    tk.Label(root, text="Enter your name:", 
             font=("Arial", 15),
             bg=theme["bg"], fg=theme["text"]).pack()
    
    tk.Entry(root, textvariable=username,
             font=("Arial", 15), width=25,
             bg="white" if CURRENT_THEME == "light" else "#2d2d2d",
             fg=theme["text"]).pack(pady=8)

    tk.Label(root, text="Enter your age:", 
             font=("Arial", 15),
             bg=theme["bg"], fg=theme["text"]).pack()
    
    tk.Entry(root, textvariable=age,
             font=("Arial", 15), width=25,
             bg="white" if CURRENT_THEME == "light" else "#2d2d2d",
             fg=theme["text"]).pack(pady=8)

    def start():
        if not username.get().strip():
            play_sound("error")
            messagebox.showwarning(
                "Name Required",
                "Please enter your name to continue."
            )
            return

        if not age.get().isdigit():
            play_sound("error")
            messagebox.showwarning(
                "Invalid Age",
                "Please enter your age using numbers only."
            )
            return

        root.destroy()
        play_sound("success")
        start_quiz(username.get(), int(age.get()))

    tk.Button(
        root, text="Start Assessment",
        command=start,
        bg=theme["primary"], fg="white",
        font=("Arial", 14, "bold"),
        width=20
    ).pack(pady=15)
    
    # Settings button added here
    tk.Button(
        root, text="⚙️ Settings",
        command=lambda: open_settings(root),
        bg=theme["secondary"], fg="white",
        font=("Arial", 12),
        width=15
    ).pack(pady=5)

    root.mainloop()

#QUIZ
def start_quiz(username, age):
    # Use QUESTION_COUNT setting
    filtered_questions = [q for q in questions if q["age_min"] <= age <= q["age_max"]][:QUESTION_COUNT]

    if not filtered_questions:
        play_sound("error")
        messagebox.showinfo(
            "No Questions Available",
            "There are currently no questions available for your age group.\n"
            "Please check back later."
        )
        return

    total_questions = len(filtered_questions)

    quiz = tk.Tk()
    quiz.title("SoulSense Quiz")
    quiz.geometry("750x600")
    quiz.resizable(False, False)
    
    theme = get_theme()
    quiz.configure(bg=theme["bg"])

    responses = []
    score = 0
    current_q = 0
    var = tk.IntVar()

    start_time = time.time()

    timer_label = tk.Label(quiz, font=("Arial", 14, "bold"), 
                          fg=theme["timer"], bg=theme["bg"])
    timer_label.pack(pady=5)

    def update_timer():
        elapsed = int(time.time() - start_time)
        m, s = divmod(elapsed, 60)
        timer_label.config(text=f"Time: {m:02d}:{s:02d}")
        quiz.after(1000, update_timer)

    update_timer()

    question_label = tk.Label(quiz, wraplength=700, font=("Arial", 16),
                             bg=theme["bg"], fg=theme["text"])
    question_label.pack(pady=20)

    for text, val in [
        ("Strongly Disagree", 1),
        ("Disagree", 2),
        ("Neutral", 3),
        ("Agree", 4),
        ("Strongly Agree", 5)
    ]:
        tk.Radiobutton(
            quiz, text=text,
            variable=var, value=val,
            font=("Arial", 14),
            bg=theme["bg"], fg=theme["text"],
            selectcolor=theme["primary"],
            activebackground=theme["bg"],
            activeforeground=theme["text"]
        ).pack(anchor="w", padx=60)

    def load_question():
        question_label.config(text=filtered_questions[current_q]["text"])

    def save_and_exit(title):
        elapsed = int(time.time() - start_time)
        analytics = compute_analytics(responses, elapsed, total_questions)

        def db_save():
            cursor.execute("""
                INSERT INTO scores
                (username, age, total_score, avg_response, max_response, min_response,
                 score_variance, questions_attempted, completion_ratio,
                 avg_time_per_question, time_taken_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                username, age, score,
                analytics["avg"], analytics["max"], analytics["min"],
                analytics["variance"], analytics["attempted"],
                analytics["completion"], analytics["avg_time"], elapsed
            ))
            conn.commit()

        try:
            retry_operation(db_save)
        except Exception:
            play_sound("error")
            messagebox.showerror(
                "Save Failed",
                "We couldn’t save your results due to a temporary issue.\n"
                "Please try again in a few moments."
            )
            quiz.destroy()
            return

        play_sound("success")
        messagebox.showinfo(
            title,
            f"Assessment Summary\n\n"
            f"Score: {score}\n"
            f"Questions Attempted: {analytics['attempted']}\n"
            f"Time Taken: {elapsed} seconds"
        )

        quiz.destroy()
        conn.close()

    def next_question():
        nonlocal current_q, score
        if var.get() == 0:
            play_sound("error")
            messagebox.showwarning(
                "Selection Required",
                "Please select an option before moving to the next question."
            )
            return

        play_sound("click")
        responses.append(var.get())
        score += var.get()
        var.set(0)
        current_q += 1

        if current_q < total_questions:
            load_question()
        else:
            save_and_exit("Assessment Completed")

    def stop_test():
        if messagebox.askyesno(
            "Stop Assessment",
            "Are you sure you want to stop the assessment?\n"
            "Your progress will be saved."
        ):
            play_sound("click")
            save_and_exit("Assessment Stopped")

    tk.Button(
        quiz, text="Next",
        command=next_question,
        bg=theme["primary"], fg="white",
        font=("Arial", 14, "bold"),
        width=15
    ).pack(pady=15)

    tk.Button(
        quiz, text="Stop Test",
        command=stop_test,
        bg=theme["warning"], fg="white",
        font=("Arial", 13, "bold"),
        width=15
    ).pack()
    
    # Settings button in quiz
    tk.Button(
        quiz, text="⚙️ Settings",
        command=lambda: open_settings(quiz),
        bg=theme["secondary"], fg="white",
        font=("Arial", 12),
        width=12
    ).pack(pady=10)

    load_question()
    quiz.mainloop()

#START APP
show_splash()