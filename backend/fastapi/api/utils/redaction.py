import re
import logging
from typing import Any, Dict, List, Union

logger = logging.getLogger(__name__)

def mask_email(email: str) -> str:
    if not email or "@" not in email:
        return "***"
    user, domain = email.split("@")
    if len(user) <= 2:
        return f"{user[0]}***@{domain}"
    return f"{user[0]}{'*' * (len(user)-2)}{user[-1]}@{domain}"

def mask_phone(phone: str) -> str:
    if not phone: return "***"
    # Basic masking for digits
    return f"{phone[:3]}***{phone[-4:]}" if len(phone) >= 7 else "***"

def mask_generic(val: Any) -> str:
    return "********"

PII_KEYS = {
    "email": mask_email,
    "phone": mask_phone,
    "phone_number": mask_phone,
    "ip_address": mask_generic,
    "ssn": mask_generic,
    "password_hash": lambda x: None # Remove completely
}

def redact_data(data: Any, viewer_roles: List[str]) -> Any:
    """
    Recursively redact PII from JSON-like data based on user roles (#1088).
    If 'admin' or 'pii_viewer' is in roles, return original.
    """
    if "admin" in viewer_roles or "pii_viewer" in viewer_roles:
        return data

    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            k_lower = k.lower()
            
            # Match keys like 'email', 'user_email', 'work_email'
            matched = False
            for pii_key, mask_func in PII_KEYS.items():
                if pii_key in k_lower:
                    new_dict[k] = mask_func(v) if v is not None else None
                    matched = True
                    break
            
            if not matched:
                new_dict[k] = redact_data(v, viewer_roles)
        return new_dict
    
    if isinstance(data, list):
        return [redact_data(item, viewer_roles) for item in data]
    
    return data
