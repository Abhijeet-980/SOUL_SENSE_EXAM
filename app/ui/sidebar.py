import tkinter as tk
from tkinter import ttk

class SidebarNav(tk.Frame):
    def __init__(self, parent, app, items, on_change=None):
        """
        Sidebar Navigation Component
        
        Args:
            parent: Parent widget
            app: App instance (for colors/styles)
            items: List of dicts {'id': str, 'icon': str, 'label': str}
            on_change: Callback function(item_id) when selection changes
        """
        super().__init__(parent, bg=app.colors.get("sidebar_bg"), width=250)
        self.app = app
        self.items = items
        self.on_change = on_change
        self.buttons = {}
        self.active_id = None
        
        # Prevent auto-shrinking
        self.pack_propagate(False)
        self.grid_propagate(False)
        
        self._render_header()
        self._render_items()
        
    def _render_header(self):
        # User Profile Summary Area
        header = tk.Frame(self, bg=self.app.colors.get("sidebar_bg"), height=100)
        header.pack(fill="x", padx=20, pady=30)
        
        # Avatar Placeholder (Circle)
        avatar_canvas = tk.Canvas(header, width=60, height=60, bg=self.app.colors.get("sidebar_bg"), highlightthickness=0)
        avatar_canvas.pack(side="left")
        
        # Draw circle
        avatar_canvas.create_oval(5, 5, 55, 55, fill="white", outline=self.app.colors.get("sidebar_divider"))
        # Initials
        initial = self.app.username[0].upper() if self.app.username else "?"
        avatar_canvas.create_text(30, 30, text=initial, font=("Segoe UI", 20, "bold"), fill=self.app.colors.get("sidebar_bg"))
        
        # Name Info
        info_frame = tk.Frame(header, bg=self.app.colors.get("sidebar_bg"))
        info_frame.pack(side="left", padx=15, fill="both", expand=True)
        
        tk.Label(
            info_frame, 
            text=self.app.username,
            font=("Segoe UI", 12, "bold"),
            bg=self.app.colors.get("sidebar_bg"),
            fg="white",
            anchor="w"
        ).pack(fill="x", pady=(10, 0))
        
        tk.Label(
            info_frame, 
            text="User Profile",
            font=("Segoe UI", 9),
            bg=self.app.colors.get("sidebar_bg"),
            fg=self.app.colors.get("sidebar_divider"),
            anchor="w"
        ).pack(fill="x")

    def _render_items(self):
        self.nav_frame = tk.Frame(self, bg=self.app.colors.get("sidebar_bg"))
        self.nav_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        for item in self.items:
            self._create_nav_item(item)
            
    def _create_nav_item(self, item):
        item_id = item["id"]
        
        btn_frame = tk.Frame(self.nav_frame, bg=self.app.colors.get("sidebar_bg"), cursor="hand2", height=50)
        btn_frame.pack(fill="x", pady=2)
        btn_frame.pack_propagate(False)
        
        # Active Indicator (Left Bar) - Hidden by default
        indicator = tk.Frame(btn_frame, bg=self.app.colors.get("sidebar_bg"), width=4)
        indicator.pack(side="left", fill="y", pady=8, padx=(0, 8))
        
        # Icon
        lbl_icon = tk.Label(
            btn_frame, 
            text=item.get("icon", "•"), 
            font=("Segoe UI", 14),
            bg=self.app.colors.get("sidebar_bg"),
            fg=self.app.colors.get("sidebar_fg")
        )
        lbl_icon.pack(side="left", padx=5)
        
        # Label
        lbl_text = tk.Label(
            btn_frame, 
            text=item.get("label", item_id.title()), 
            font=("Segoe UI", 11),
            bg=self.app.colors.get("sidebar_bg"),
            fg=self.app.colors.get("sidebar_fg")
        )
        lbl_text.pack(side="left", padx=10)
        
        # Store references
        self.buttons[item_id] = {
            "frame": btn_frame,
            "indicator": indicator,
            "icon": lbl_icon,
            "text": lbl_text
        }
        
        # Bind events
        for widget in [btn_frame, lbl_icon, lbl_text, indicator]:
            widget.bind("<Button-1>", lambda e, i=item_id: self.select_item(i))
            widget.bind("<Enter>", lambda e, i=item_id: self._on_hover(i, True))
            widget.bind("<Leave>", lambda e, i=item_id: self._on_hover(i, False))
            
    def _on_hover(self, item_id, is_hovering):
        if item_id == self.active_id:
            return
            
        widgets = self.buttons[item_id]
        bg_color = self.app.colors.get("sidebar_hover") if is_hovering else self.app.colors.get("sidebar_bg")
        
        widgets["frame"].configure(bg=bg_color)
        widgets["icon"].configure(bg=bg_color)
        widgets["text"].configure(bg=bg_color)
        widgets["indicator"].configure(bg=bg_color)

    def select_item(self, item_id):
        if self.active_id == item_id:
            return
            
        # Reset old active
        if self.active_id:
            self._update_item_style(self.active_id, False)
            
        # Set new active
        self.active_id = item_id
        self._update_item_style(item_id, True)
        
        if self.on_change:
            self.on_change(item_id)
            
    def _update_item_style(self, item_id, is_active):
        widgets = self.buttons[item_id]
        
        # Colors
        if is_active:
            bg_color = self.app.colors.get("sidebar_active")
            fg_color = "white"
            indicator_color = "white"
        else:
            bg_color = self.app.colors.get("sidebar_bg")
            fg_color = self.app.colors.get("sidebar_fg")
            indicator_color = bg_color # Hide by matching bg
            
        widgets["frame"].configure(bg=bg_color)
        widgets["icon"].configure(bg=bg_color, fg=fg_color)
        widgets["text"].configure(bg=bg_color, fg=fg_color)
        widgets["indicator"].configure(bg=indicator_color)
