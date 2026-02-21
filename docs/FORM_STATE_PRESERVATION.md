"""
Form State Preservation Best Practices

This document provides guidelines for ensuring form data is never lost due to
validation errors in the SoulSense application.
"""

# ==============================================================================
# OVERVIEW
# ==============================================================================
# 
# Form State Preservation ensures that when a user fills out a form and
# encounters a validation error, all their entered data remains visible and
# editable. This improves the user experience by:
# 
# 1. Reducing Frustration: Users don't have to re-enter all data
# 2. Context Preservation: Users can see exactly which fields have errors
# 3. Confidence Building: Visual feedback that their efforts are preserved
# 4. Accessibility: Easier navigation between error and fix

# ==============================================================================
# KEY PRINCIPLES
# ==============================================================================

print("""
KEY PRINCIPLES FOR FORM STATE PRESERVATION:

1. NEVER USE MESSAGEBOX FOR VALIDATION ERRORS
   - Messaging boxes steal focus and hide form context
   - Use inline error labels instead
   - Keep the form visible and editable

2. PRESERVE DATA IN ENTRY WIDGETS 
   - Don't clear fields when validation fails
   - Keep content until user explicitly clears it
   - Allow users to edit and resubmit

3. HIGHLIGHT PROBLEMATIC FIELDS
   - Use red border/highlight for fields with errors
   - Use green border/highlight for valid fields
   - Reset to neutral when error is cleared

4. SHOW SPECIFIC ERROR MESSAGES INLINE
   - Display error text directly below the field
   - Use consistent error styling (#EF4444 red)
   - Reference the field label in the error message

5. FOCUS FIRST FIELD WITH ERROR
   - Move cursor to the first problematic field
   - Helps user understand where to look
   - Improves form navigation

6. VALIDATE INCREMENTALLY
   - Validate all fields before submission
   - Collect all errors before displaying them
   - Show all errors at once for clarity
""")

# ==============================================================================
# IMPLEMENTATION PATTERNS
# ==============================================================================

# Pattern 1: INLINE ERROR LABELS (PREFERRED)
# Used in: Registration Form, Login Form

pattern_inline_errors = """
# Create error label for each field:
error_label = tk.Label(parent_frame, text="", font=("Segoe UI", 8), 
                       bg="#FFFFFF", fg="#EF4444")
error_label.pack(anchor="w")

# During validation:
if not email:
    error_label.config(text="Email is required")
    email_entry.config(highlightbackground="#EF4444", highlightcolor="#EF4444")
else:
    error_label.config(text="")
    email_entry.config(highlightbackground="#10B981", highlightcolor="#10B981")

# Key Benefits:
# - Form stays visible and context-aware
# - User can immediately see and fix errors
# - Error specific to the field it describes
# - No dialog box interruption
"""

# Pattern 2: VALIDATION STATE TRACKING
# Used in: FormStateManager utility

pattern_state_tracking = """
# Initialize form state tracker:
form_state = FormState()
form_state.add_field("email", widget=email_entry, error_label=email_error_label)

# Update validation status:
form_state.set_field_error("email", "Invalid email format")

# Check for any errors:
if form_state.has_errors():
    # Focus first field with error
    first_error = next((f for f in form_state.fields.values() if f.error), None)
    if first_error:
        first_error.widget.focus_set()
    return  # Don't submit

# Get all cleaned data:
clean_data = form_state.get_all_values()
"""

# Pattern 3: FIELD HIGHLIGHTING FOR VISUAL FEEDBACK
# Used in: All forms

pattern_highlighting = """
# Color codes for field states:
ERROR_COLOR = "#EF4444"      # Red - field has error
SUCCESS_COLOR = "#10B981"    # Green - field validates
NORMAL_COLOR = "#E2E8F0"     # Gray - neutral/not yet validated

# Apply highlighting:
def update_field_state(entry_widget, is_valid):
    if is_valid is None:  # Not yet validated
        entry_widget.config(highlightbackground=NORMAL_COLOR, 
                          highlightcolor=NORMAL_COLOR)
    elif is_valid:  # Validation passed
        entry_widget.config(highlightbackground=SUCCESS_COLOR, 
                          highlightcolor=SUCCESS_COLOR)
    else:  # Validation failed
        entry_widget.config(highlightbackground=ERROR_COLOR, 
                          highlightcolor=ERROR_COLOR)
"""

# ==============================================================================
# FORM VALIDATION FLOW
# ==============================================================================

validation_flow = """
VALIDATION FLOW FOR FORM SUBMISSION:

1. USER CLICKS SUBMIT/LOGIN/REGISTER
   ↓
2. COLLECT ALL FIELD VALUES
   ↓
3. CLEAR ALL PREVIOUS ERRORS
   └─ Reset all error labels to empty ""
   └─ Reset all field highlights to neutral
   ↓
4. VALIDATE EACH FIELD
   └─ If field has error:
      ├─ Show error message in error_label
      ├─ Highlight field in red
      └─ Track field_with_error for later focus
   └─ If field is valid:
      ├─ Clear error message
      └─ Highlight field in green (or neutral)
   ↓
5. CHECK IF ANY ERRORS EXIST
   └─ If has_errors == True:
      ├─ Focus first field with error
      ├─ **RETURN WITHOUT CLEARING DATA**
      └─ Form remains visible and editable
   └─ If has_errors == False:
      └─ Proceed to step 6
   ↓
6. SUBMIT FORM DATA TO BACKEND
   ↓
7. HANDLE BACKEND RESPONSE
   └─ If success:
      └─ Close form and show success message
   └─ If failure (duplicate username, etc):
      └─ Show error but keep form open
      └─ Data remains preserved for user to edit
"""

# ==============================================================================
# REAL-TIME VALIDATION
# ==============================================================================

realtime_validation = """
REAL-TIME (AS-YOU-TYPE) VALIDATION:

Used for: Email, Phone, Password, etc.

Binding:
entry_widget.bind("<KeyRelease>", validate_email_realtime)
entry_widget.bind("<FocusOut>", validate_email_realtime)

Behavior:
- Validates WHILE typing (not just on submit)
- Shows error immediately if field invalid
- Shows success (green) when field becomes valid
- Suggests corrections (e.g., "Did you mean gmail.com?")

Example: Email validation shows:
- Red error message immediately if format invalid
- Green checkmark when email becomes valid
- Suggestion popup if domain typo detected

Benefits:
- Users know immediately if input is correct
- Can fix errors before form submission
- Reduces submission errors
- Better user confidence
"""

# ==============================================================================
# ERROR MESSAGE GUIDELINES
# ==============================================================================

error_messages = """
ERROR MESSAGE GUIDELINES:

1. BE SPECIFIC
   ✓ "Email must be valid (e.g., user@example.com)"
   ✗ "Invalid input"

2. BE ACTIONABLE
   ✓ "Age must be between 13 and 120"
   ✗ "Age error"

3. BE FRIENDLY
   ✓ "This password is too common. Try adding numbers or symbols."
   ✗ "WEAK PASSWORD"

4. INCLUDE FIELD CONTEXT
   ✓ "Username must start with a letter"
   ✗ "Invalid"

5. AVOID JARGON
   ✓ "Email already registered - use Sign In instead"
   ✗ "DUPLICATE_EMAIL_ERR"

6. OFFER SOLUTIONS
   ✓ "Did you mean username1 (that username is taken)?"
   ✗ "Username taken"
"""

# ==============================================================================
# ACCESSIBILITY CONSIDERATIONS
# ==============================================================================

accessibility = """
ACCESSIBILITY BEST PRACTICES:

1. ERROR LABELS HAVE PROPER COLORS
   - Red (#EF4444) for errors (but not ONLY red)
   - Use icons or text to indicate error status
   - Important: 4.5:1 contrast ratio with background

2. FOCUS MANAGEMENT
   - Focus moves to first field with error automatically
   - Screen readers announce error messages
   - Keyboard navigation works throughout form

3. SEMANTIC HTML/WIDGETS
   - Use proper widget types (Entry not Label for input)
   - Associate labels with input fields
   - Use proper font sizes for readability

4. ERROR ANNOUNCEMENT
   - Errors announced (not just visually shown)
   - Status updates announced for real-time validation
   - Form submission status announced

5. MULTIPLE INDICATORS
   - Don't rely on color alone
   - Use text ("Error: ..."), icons, borders
   - Works for colorblind users too
"""

# ==============================================================================
# FORMS THAT USE THIS PATTERN
# ==============================================================================

forms_using_pattern = """
FORMS WITH FORM STATE PRESERVATION IMPLEMENTED:

1. LOGIN FORM (app/auth/app_auth.py - show_login_screen)
   ✓ Inline error labels for all fields
   ✓ Data preserved on validation failure
   ✓ Real-time email validation with domain suggestions
   ✓ Field highlighting
   
2. REGISTRATION FORM (app/auth/app_auth.py - show_signup_screen)
   ✓ Inline error labels for all fields
   ✓ Data preserved on validation failure
   ✓ Real-time password strength meter
   ✓ Password confirmation validation
   ✓ Real-time email validation
   ✓ Comprehensive field highlighting

3. PROFILE FORM (app/ui/profile.py - save_personal_data)
   [CANDIDATE FOR UPGRADE - currently uses messagebox]
   - Should convert to inline errors
   - Should add field highlighting
   - Should preserve form visible on error

4. EXPORT DIALOG (app/ui/export_dialog.py)
   [CANDIDATE FOR UPGRADE]
   
5. SETTINGS FORM
   [CANDIDATE FOR UPGRADE]
"""

# ==============================================================================
# HOW TO USE THE FORM STATE MANAGER
# ==============================================================================

form_manager_usage = """
USING FormStateManager:

# In form initialization:
from app.ui.form_state_manager import get_form_state_manager

manager = get_form_state_manager()
form_state = manager.create_form("registration_form")

# Register fields:
manager.register_field("registration_form", "email", 
                      widget=email_entry, error_label=email_error_label)

# During validation:
def validate():
    # Validate email
    is_valid, error = validate_email_function(email_entry.get())
    manager.set_field_error("registration_form", "email", 
                           error if not is_valid else "")
    
    # Validate all
    if manager.has_errors("registration_form"):
        return False  # Stay on form
    
    # Get clean data
    data = manager.get_form_values("registration_form")
    # Submit data...
"""

# ==============================================================================
# TESTING FORM STATE PRESERVATION
# ==============================================================================

testing = """
HOW TO TEST FORM STATE PRESERVATION:

1. FILL FORM PARTIALLY
   - Enter data in first few fields
   - Leave rest empty
   - Click submit

   EXPECTED: Form still shows with:
   ✓ All entered data visible
   ✓ Error messages for empty fields
   ✓ Fields highlighted in red

2. CORRECT ONE ERROR AT A TIME
   - Fill one empty field
   - Click submit again
   - Repeat

   EXPECTED:
   ✓ Previous correct data still visible
   ✓ Only remaining errors shown
   ✓ Fixed fields highlighted in green

3. TRY INVALID DATA
   - Enter invalid email
   - Enter age 200
   - Enter weak password
   - Click submit

   EXPECTED:
   ✓ All data preserved
   ✓ Specific error for each field
   ✓ User can edit without re-entering everything

4. VALIDATE REAL-TIME
   - As you type in email field
   - Change password strength
   - Confirm password match

   EXPECTED:
   ✓ Errors appear as you type
   ✓ Success indicators appear when valid
   ✓ Form remains editable
"""

# ==============================================================================
# COMMON MISTAKES TO AVOID
# ==============================================================================

mistakes = """
COMMON MISTAKES TO AVOID:

❌ MISTAKE 1: Clearing form on error
   entry_field.delete(0, tk.END)  # DON'T DO THIS!
   
   ✓ CORRECT: Leave data in entry
   # Data stays in entry_field.get()

❌ MISTAKE 2: Using only messagebox for errors
   messagebox.showerror("Error", "Field required")
   
   ✓ CORRECT: Use inline errors
   error_label.config(text="Field required")

❌ MISTAKE 3: Not focusing problematic field
   # User doesn't know which field has error
   
   ✓ CORRECT: Focus first field with error
   entry_field.focus_set()

❌ MISTAKE 4: Clearing errors before showing new ones
   # Uses old valid state instead of showing errors
   error_label.config(text="")  # Too early!
   
   ✓ CORRECT: Batch clear then validate
   error_label.config(text="")  # Before validation
   # ... validate all fields ...
   error_label.config(text=error_msg if has_error else "")

❌ MISTAKE 5: Only showing first error
   # User has multiple issues, fixes one, finds more
   
   ✓ CORRECT: Show all errors at once
   # Validate all fields
   # Display all error messages
   # User can fix multiple issues in one pass

❌ MISTAKE 6: Hidden errors
   # Error message cut off or scrolled away
   
   ✓ CORRECT: Ensure visibility
   # Place error labels right below fields
   # Scroll form to ensure error is visible
   # Use clear, readable fonts
"""

print("Form State Preservation Documentation Loaded")
print("")
print("Key Files:")
print("- app/ui/form_state_manager.py: Form state utility")
print("- app/auth/app_auth.py: Login & Registration forms")
print("- app/ui/profile.py: Profile form (candidate for upgrade)")
print("")
print("For detailed implementation, see the docstrings in each module.")
