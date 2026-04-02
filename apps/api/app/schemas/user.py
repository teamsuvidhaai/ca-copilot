import re
from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, EmailStr, field_validator

from app.models.models import UserRole, SignupMethod, AccountType

# ───────────────────────────────────────
# Shared properties
# ───────────────────────────────────────
class UserBase(BaseModel):
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    phone_number: Optional[str] = None
    job_title: Optional[str] = None
    subscription_plan: Optional[str] = "free"
    role: UserRole = UserRole.STAFF

# ───────────────────────────────────────
# Validators (reusable)
# ───────────────────────────────────────
def validate_phone_number(value: str) -> str:
    """
    Accepts:
      - 10-digit Indian mobile number: 9876543210
      - With country code: +919876543210
      - With spaces/dashes: +91 98765 43210, +91-9876543210
    """
    if value is None:
        return value
    # Strip spaces, dashes, parentheses
    cleaned = re.sub(r'[\s\-\(\)]+', '', value)
    # Pattern: optional +, 1-3 digit country code, then 10 digits (Indian)
    # Also allow generic international: + followed by 7 to 15 digits
    pattern = r'^\+?(\d{1,3})?\d{10}$'
    if not re.match(pattern, cleaned):
        raise ValueError(
            'Invalid phone number. Use 10-digit number (e.g. 9876543210) '
            'or international format (e.g. +919876543210)'
        )
    return cleaned


def validate_password_strength(value: str) -> str:
    """Password must be >= 8 chars, contain at least 1 letter and 1 digit."""
    if len(value) < 8:
        raise ValueError('Password must be at least 8 characters long')
    if not re.search(r'[A-Za-z]', value):
        raise ValueError('Password must contain at least one letter')
    if not re.search(r'\d', value):
        raise ValueError('Password must contain at least one number')
    return value


# ───────────────────────────────────────
# Email Signup (existing flow)
# ───────────────────────────────────────
class UserCreate(UserBase):
    """Sign up with email + password (requires OTP verification)."""
    email: EmailStr
    password: str
    firm_name: str  # Used during signup to create the firm
    account_type: str = "ca_firm"  # 'ca_firm' or 'corporate'
    verification_token: Optional[str] = None  # From OTP verification step

    @field_validator('password')
    @classmethod
    def check_password(cls, v):
        return validate_password_strength(v)


# ───────────────────────────────────────
# Phone Signup
# ───────────────────────────────────────
class UserCreatePhone(BaseModel):
    """Sign up with phone number + password (requires OTP verification)."""
    phone_number: str
    password: str
    full_name: str
    firm_name: str
    account_type: str = "ca_firm"  # 'ca_firm' or 'corporate'
    email: Optional[EmailStr] = None  # Optional – can add later
    verification_token: Optional[str] = None  # From OTP verification step

    @field_validator('phone_number')
    @classmethod
    def check_phone(cls, v):
        return validate_phone_number(v)

    @field_validator('password')
    @classmethod
    def check_password(cls, v):
        return validate_password_strength(v)


# ───────────────────────────────────────
# Google Signup
# ───────────────────────────────────────
class UserCreateGoogle(BaseModel):
    """Sign up / login with Google OAuth id_token."""
    google_id_token: str
    firm_name: Optional[str] = None  # Optional for returning users
    account_type: str = "ca_firm"  # 'ca_firm' or 'corporate'


# ───────────────────────────────────────
# Update schemas
# ───────────────────────────────────────
class UserUpdate(UserBase):
    password: Optional[str] = None

class UserProfileUpdate(BaseModel):
    full_name: Optional[str] = None
    email: Optional[EmailStr] = None
    firm_name: Optional[str] = None
    phone_number: Optional[str] = None
    job_title: Optional[str] = None
    subscription_plan: Optional[str] = None

    @field_validator('phone_number')
    @classmethod
    def check_phone(cls, v):
        if v is not None:
            return validate_phone_number(v)
        return v


# ───────────────────────────────────────
# DB / Response schemas
# ───────────────────────────────────────
class UserInDBBase(UserBase):
    id: UUID
    firm_id: UUID
    signup_method: Optional[SignupMethod] = SignupMethod.EMAIL
    created_at: Optional[datetime] = None
    trial_started_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# Additional properties to return via API
class User(UserInDBBase):
    firm_name: Optional[str] = None
    account_type: Optional[str] = "ca_firm"

class UserInDB(UserInDBBase):
    hashed_password: Optional[str] = None
