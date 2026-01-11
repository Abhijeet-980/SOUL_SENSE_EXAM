"""
Soul Sense Exam Module
Premium UI with modern question cards and progress tracking
"""

import tkinter as tk
from tkinter import ttk, messagebox
import logging
import time
from datetime import datetime
import statistics
from app.db import get_connection
from app.utils import compute_age_group


class ExamManager:
    """Manages the exam/question flow with premium styling"""
    
    def __init__(self, app):
        self.app = app
        self.root = app.root
        self.answer_var = tk.IntVar()

    def start_test(self):
        """Initialize test state and start the exam"""
        from app.services.exam_service import ExamSession
        
        # Init or Reset ExamSession
        # We need to pass the current set of questions to the session
        # Ensure questions are in the format expected by ExamSession: (id, text, tooltip, min, max) or (text, tooltip)
        # self.app.questions is [(id, text, tooltip, min, max)...] or [(text, tooltip)...]
        
        self.session = ExamSession(
            username=self.app.username,
            age=self.app.age,
            age_group=self.app.age_group,
            questions=self.app.questions
        )
        self.session.start_exam()
        
        # Sync simple state for UI if needed (though we should read from session)
        self.app.current_question = 0
        self.app.responses = []
        
        self.show_question()

    def show_question(self):
        """Display current question with premium styling"""
        self.app.clear_screen()
        
        colors = self.app.colors
        
        # Check if test is complete using Session
        if self.session.is_finished():
            self.show_reflection_screen()
            return
        
        # Get data from Session
        q_text, q_tooltip = self.session.get_current_question()
        current_idx, total_count, progress_pct = self.session.get_progress()
        
        # Update app state for backward compat
        self.app.current_question = current_idx - 1
        
        # Main container
        main_frame = tk.Frame(self.root, bg=colors["bg"])
        main_frame.pack(fill="both", expand=True)
        
        # Progress Header
        progress_frame = tk.Frame(main_frame, bg=colors.get("bg_secondary", "#F1F5F9"), height=60)
        progress_frame.pack(fill="x")
        progress_frame.pack_propagate(False)
        
        progress_inner = tk.Frame(progress_frame, bg=colors.get("bg_secondary", "#F1F5F9"))
        progress_inner.pack(fill="x", padx=30, pady=15)
        
        # Question Counter
        counter_label = tk.Label(
            progress_inner,
            text=f"Question {current_idx} of {total_count}",
            font=("Segoe UI", 12, "bold"),
            bg=colors.get("bg_secondary", "#F1F5F9"),
            fg=colors.get("text_primary", "#0F172A")
        )
        counter_label.pack(side="left")
        
        # Progress percentage
        pct_label = tk.Label(
            progress_inner,
            text=f"{int(progress_pct)}% Complete",
            font=("Segoe UI", 11),
            bg=colors.get("bg_secondary", "#F1F5F9"),
            fg=colors.get("text_secondary", "#475569")
        )
        pct_label.pack(side="right")
        
        # Progress Bar
        progress_bar_frame = tk.Frame(main_frame, bg=colors["bg"], height=8)
        progress_bar_frame.pack(fill="x")
        
        style = ttk.Style()
        style.configure(
            "Exam.Horizontal.TProgressbar",
            troughcolor=colors.get("bg_secondary", "#F1F5F9"),
            background=colors.get("primary", "#3B82F6"),
            thickness=6
        )
        
        progress_bar = ttk.Progressbar(
            progress_bar_frame,
            style="Exam.Horizontal.TProgressbar",
            orient="horizontal",
            mode="determinate",
            value=progress_pct
        )
        progress_bar.pack(fill="x")
        
        # Content Area
        content_frame = tk.Frame(main_frame, bg=colors["bg"])
        content_frame.pack(fill="both", expand=True, padx=40, pady=20)
        
        # Question Card
        question_card = tk.Frame(
            content_frame,
            bg=colors.get("surface", "#FFFFFF"),
            highlightbackground=colors.get("border", "#E2E8F0"),
            highlightthickness=1
        )
        question_card.pack(fill="x", pady=10)
        
        card_inner = tk.Frame(question_card, bg=colors.get("surface", "#FFFFFF"))
        card_inner.pack(fill="x", padx=25, pady=20)
        
        # Question Header with tooltip
        q_header_frame = tk.Frame(card_inner, bg=colors.get("surface", "#FFFFFF"))
        q_header_frame.pack(fill="x")
        
        question_label = tk.Label(
            q_header_frame,
            text=q_text,
            font=("Segoe UI", 14),
            bg=colors.get("surface", "#FFFFFF"),
            fg=colors.get("text_primary", "#0F172A"),
            wraplength=480,
            justify="left"
        )
        question_label.pack(side="left", anchor="w")
        
        # Tooltip Icon
        tooltip_val = q_tooltip if q_tooltip else "Select the option that best describes you."
        
        info_btn = tk.Button(
            q_header_frame,
            text="ℹ️",
            font=("Segoe UI", 12),
            bg=colors.get("surface", "#FFFFFF"),
            fg=colors.get("text_tertiary", "#94A3B8"),
            relief="flat",
            activebackground=colors.get("surface", "#FFFFFF"),
            activeforeground=colors.get("primary", "#3B82F6"),
            bd=0,
            cursor="hand2",
            command=lambda: self._show_tooltip(tooltip_val)
        )
        info_btn.pack(side="right", padx=5)
        
        # Options Section
        options_frame = tk.Frame(card_inner, bg=colors.get("surface", "#FFFFFF"))
        options_frame.pack(fill="x", pady=(20, 5))
        
        self.answer_var.set(0)  # Reset selection
        
        options = [
            ("Never", 1),
            ("Sometimes", 2),
            ("Often", 3),
            ("Always", 4)
        ]
        
        # Create styled radio buttons
        self.option_buttons = []
        for text, value in options:
            option_frame = tk.Frame(
                options_frame,
                bg=colors.get("surface", "#FFFFFF"),
                highlightbackground=colors.get("border", "#E2E8F0"),
                highlightthickness=1,
                cursor="hand2"
            )
            option_frame.pack(fill="x", pady=4)
            
            rb = tk.Radiobutton(
                option_frame,
                text=text,
                variable=self.answer_var,
                value=value,
                font=("Segoe UI", 12),
                bg=colors.get("surface", "#FFFFFF"),
                fg=colors.get("text_primary", "#0F172A"),
                selectcolor=colors.get("primary_light", "#DBEAFE"),
                activebackground=colors.get("surface_hover", "#F8FAFC"),
                activeforeground=colors.get("text_primary", "#0F172A"),
                indicatoron=True,
                padx=15,
                pady=10,
                anchor="w"
            )
            rb.pack(fill="x")
            
            # Hover effects on option frame
            def on_enter(e, frame=option_frame):
                frame.configure(highlightbackground=colors.get("primary", "#3B82F6"))
            
            def on_leave(e, frame=option_frame):
                frame.configure(highlightbackground=colors.get("border", "#E2E8F0"))
            
            option_frame.bind("<Enter>", on_enter)
            option_frame.bind("<Leave>", on_leave)
            rb.bind("<Enter>", on_enter)
            rb.bind("<Leave>", on_leave)
            
            self.option_buttons.append((option_frame, rb))
        
        # Navigation Buttons
        nav_frame = tk.Frame(content_frame, bg=colors["bg"])
        nav_frame.pack(pady=20)
        
        # Back Button (if not first question)
        if current_idx > 1:
            back_btn = tk.Button(
                nav_frame,
                text="← Previous",
                command=self.previous_question,
                font=("Segoe UI", 11),
                bg=colors.get("surface", "#FFFFFF"),
                fg=colors.get("text_secondary", "#475569"),
                activebackground=colors.get("surface_hover", "#F8FAFC"),
                activeforeground=colors.get("text_primary", "#0F172A"),
                relief="flat",
                cursor="hand2",
                width=12,
                pady=8,
                borderwidth=1,
                highlightbackground=colors.get("border", "#E2E8F0")
            )
            back_btn.pack(side="left", padx=5)
            back_btn.bind("<Enter>", lambda e: back_btn.configure(bg=colors.get("surface_hover", "#F8FAFC")))
            back_btn.bind("<Leave>", lambda e: back_btn.configure(bg=colors.get("surface", "#FFFFFF")))
        
        # Next/Finish Button
        is_last = current_idx >= total_count
        next_text = "Finish →" if is_last else "Next →"
        
        next_btn = tk.Button(
            nav_frame,
            text=next_text,
            command=self.save_answer,
            font=("Segoe UI", 12, "bold"),
            bg=colors.get("primary", "#3B82F6"),
            fg=colors.get("text_inverse", "#FFFFFF"),
            activebackground=colors.get("primary_hover", "#2563EB"),
            activeforeground=colors.get("text_inverse", "#FFFFFF"),
            relief="flat",
            cursor="hand2",
            width=14,
            pady=10,
            borderwidth=0
        )
        next_btn.pack(side="left", padx=5)
        next_btn.bind("<Enter>", lambda e: next_btn.configure(bg=colors.get("primary_hover", "#2563EB")))
        next_btn.bind("<Leave>", lambda e: next_btn.configure(bg=colors.get("primary", "#3B82F6")))
        
        # Bind Enter key
        self.root.bind("<Return>", lambda e: self.save_answer())

    def _show_tooltip(self, text):
        """Show a simple tooltip message"""
        messagebox.showinfo("Tip", text)

    def previous_question(self):
        """Go to previous question"""
        if self.session.go_back():
            self.show_question()

    def save_answer(self):
        """Save current answer and proceed"""
        ans = self.answer_var.get()
        if ans == 0:
            messagebox.showwarning("Select an Answer", "Please select an option before continuing.")
            return
        
        try:
            self.session.submit_answer(ans)
            self.show_question()
        except ValueError as e:
            messagebox.showerror("Error", str(e))

    def show_reflection_screen(self):
        """Show premium reflection screen"""
        self.app.clear_screen()
        
        colors = self.app.colors
        
        # Main container
        main_frame = tk.Frame(self.root, bg=colors["bg"])
        main_frame.pack(fill="both", expand=True)
        
        # Header
        header_frame = tk.Frame(main_frame, bg=colors.get("secondary", "#8B5CF6"), height=100)
        header_frame.pack(fill="x")
        header_frame.pack_propagate(False)
        
        header_label = tk.Label(
            header_frame,
            text="🌟 Final Reflection",
            font=("Segoe UI", 28, "bold"),
            bg=colors.get("secondary", "#8B5CF6"),
            fg=colors.get("text_inverse", "#FFFFFF")
        )
        header_label.pack(pady=30)
        
        # Content
        content_frame = tk.Frame(main_frame, bg=colors["bg"])
        content_frame.pack(fill="both", expand=True, padx=40, pady=20)
        
        # Instruction Card
        instruction_card = tk.Frame(
            content_frame,
            bg=colors.get("surface", "#FFFFFF"),
            highlightbackground=colors.get("border", "#E2E8F0"),
            highlightthickness=1
        )
        instruction_card.pack(fill="x", pady=10)
        
        card_inner = tk.Frame(instruction_card, bg=colors.get("surface", "#FFFFFF"))
        card_inner.pack(fill="x", padx=25, pady=20)
        
        instruction_label = tk.Label(
            card_inner,
            text="Describe a recent situation where you felt emotionally challenged.\nHow did you handle it?",
            font=("Segoe UI", 13),
            bg=colors.get("surface", "#FFFFFF"),
            fg=colors.get("text_primary", "#0F172A"),
            wraplength=500,
            justify="center"
        )
        instruction_label.pack()
        
        # Text Area Frame
        text_frame = tk.Frame(content_frame, bg=colors["bg"])
        text_frame.pack(fill="both", expand=True, pady=10)
        
        self.reflection_entry = tk.Text(
            text_frame,
            height=8,
            font=("Segoe UI", 12),
            bg=colors.get("surface", "#FFFFFF"),
            fg=colors.get("text_primary", "#0F172A"),
            insertbackground=colors.get("text_primary", "#0F172A"),
            relief="flat",
            highlightthickness=2,
            highlightbackground=colors.get("border", "#E2E8F0"),
            highlightcolor=colors.get("primary", "#3B82F6"),
            padx=15,
            pady=15
        )
        self.reflection_entry.pack(fill="both", expand=True)
        
        # Submit Button
        btn_frame = tk.Frame(content_frame, bg=colors["bg"])
        btn_frame.pack(pady=15)
        
        submit_btn = tk.Button(
            btn_frame,
            text="✨ Submit & See Results",
            command=self.submit_reflection,
            font=("Segoe UI", 14, "bold"),
            bg=colors.get("success", "#10B981"),
            fg=colors.get("text_inverse", "#FFFFFF"),
            activebackground=colors.get("success_hover", "#059669"),
            activeforeground=colors.get("text_inverse", "#FFFFFF"),
            relief="flat",
            cursor="hand2",
            width=22,
            pady=12,
            borderwidth=0
        )
        submit_btn.pack()
        submit_btn.bind("<Enter>", lambda e: submit_btn.configure(bg=colors.get("success_hover", "#059669")))
        submit_btn.bind("<Leave>", lambda e: submit_btn.configure(bg=colors.get("success", "#10B981")))
        
        # Skip link
        skip_label = tk.Label(
            btn_frame,
            text="Skip this step",
            font=("Segoe UI", 10, "underline"),
            bg=colors["bg"],
            fg=colors.get("text_tertiary", "#94A3B8"),
            cursor="hand2"
        )
        skip_label.pack(pady=10)
        skip_label.bind("<Button-1>", lambda e: self._skip_reflection())

    def _skip_reflection(self):
        """Skip reflection and finish test"""
        self.session.reflection_text = ""
        self.finish_test()

    def submit_reflection(self):
        """Analyze reflection text and finish test"""
        text = self.reflection_entry.get("1.0", tk.END).strip()
        
        if not text:
            if not messagebox.askyesno("Skip?", "You haven't written anything. Do you want to skip?"):
                return
            self.session.submit_reflection("")
        else:
            # Use main app's analyzer if available
            analyzer = getattr(self.app, 'sia', None)
            self.session.submit_reflection(text, analyzer)
        
        self.finish_test()

    def finish_test(self):
        """Calculate final score and save to database"""
        success = self.session.finish_exam()
        
        if not success:
            messagebox.showerror("Error", "Failed to save exam results.")
        
        # Sync back to Main App for compatibility with ResultsManager
        self.app.current_score = self.session.score
        self.app.responses = self.session.responses
        self.app.response_times = self.session.response_times
        self.app.current_max_score = len(self.session.responses) * 4
        self.app.current_percentage = (self.app.current_score / self.app.current_max_score) * 100 if self.app.current_max_score > 0 else 0
        self.app.sentiment_score = self.session.sentiment_score
        self.app.reflection_text = self.session.reflection_text
        
        self.app.results.show_visual_results()
