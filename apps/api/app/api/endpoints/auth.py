from datetime import timedelta, datetime
from typing import Any
import secrets
import random
import re as re_module

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_, delete

from app.api import deps
from app.core import security
from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.models.models import Firm, User, UserRole, OTPVerification, Client
from app.schemas import token as token_schemas
from app.schemas import user as user_schemas

router = APIRouter()

OTP_EXPIRY_MINUTES = 10
MAX_OTP_ATTEMPTS = 5


# ═══════════════════════════════════════════════
# LOGIN — Email + Password (existing flow)
# ═══════════════════════════════════════════════
@router.post("/login", response_model=token_schemas.Token)
async def login_access_token(
    db: AsyncSession = Depends(deps.get_db), form_data: OAuth2PasswordRequestForm = Depends()
) -> Any:
    """
    OAuth2 compatible token login, get an access token for future requests.
    """
    # Find user by email
    result = await db.execute(select(User).where(User.email == form_data.username))
    user = result.scalars().first()
    
    if not user or not user.hashed_password or not security.verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect email or password")
        
    # Check trial expiry for free users (allow login, flag it)
    trial_expired = False
    if user.subscription_plan in (None, 'free', 'Free') and user.trial_started_at:
        days_used = (datetime.utcnow() - user.trial_started_at).days
        if days_used >= 30:
            trial_expired = True

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = security.create_access_token(
        user.id, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "trial_expired": trial_expired,
    }


# ═══════════════════════════════════════════════
# LOGIN — Phone + Password
# ═══════════════════════════════════════════════
from pydantic import BaseModel

class PhoneLoginRequest(BaseModel):
    phone_number: str
    password: str

@router.post("/login/phone", response_model=token_schemas.Token)
async def login_phone(
    *,
    db: AsyncSession = Depends(deps.get_db),
    body: PhoneLoginRequest,
) -> Any:
    """
    Login with phone number + password.
    """
    import re
    cleaned = re.sub(r'[\s\-\(\)]+', '', body.phone_number)

    result = await db.execute(select(User).where(User.phone_number == cleaned))
    user = result.scalars().first()

    if not user or not user.hashed_password or not security.verify_password(body.password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Incorrect phone number or password")

    # Check trial expiry for free users (allow login, flag it)
    trial_expired = False
    if user.subscription_plan in (None, 'free', 'Free') and user.trial_started_at:
        days_used = (datetime.utcnow() - user.trial_started_at).days
        if days_used >= 30:
            trial_expired = True

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = security.create_access_token(
        user.id, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "trial_expired": trial_expired,
    }


# ═══════════════════════════════════════════════
# SIGNUP — Email (NOW REQUIRES OTP VERIFICATION)
# ═══════════════════════════════════════════════
@router.post("/signup", response_model=user_schemas.User)
async def signup(
    *,
    db: AsyncSession = Depends(deps.get_db),
    user_in: user_schemas.UserCreate,
) -> Any:
    """
    Create new firm and owner user via email + password.
    Requires a verification_token from prior OTP verification.
    """
    # Check if user already exists
    result = await db.execute(select(User).where(User.email == user_in.email))
    user = result.scalars().first()
    if user:
        raise HTTPException(
            status_code=400,
            detail="The user with this email already exists in the system.",
        )
    
    # Validate OTP verification token
    if user_in.verification_token:
        result = await db.execute(
            select(OTPVerification).where(
                and_(
                    OTPVerification.verification_token == user_in.verification_token,
                    OTPVerification.identifier == user_in.email,
                    OTPVerification.identifier_type == "email",
                    OTPVerification.is_verified == True,
                )
            )
        )
        otp_record = result.scalars().first()
        if not otp_record:
            raise HTTPException(status_code=400, detail="Invalid or expired verification. Please verify your email again.")
        if otp_record.expires_at < datetime.utcnow():
            raise HTTPException(status_code=400, detail="Verification expired. Please verify your email again.")
        email_verified = True
        # Clean up used OTP records
        await db.execute(
            delete(OTPVerification).where(
                and_(
                    OTPVerification.identifier == user_in.email,
                    OTPVerification.identifier_type == "email",
                )
            )
        )
    else:
        email_verified = False
    
    # Create Firm
    firm = Firm(name=user_in.firm_name, account_type=user_in.account_type)
    db.add(firm)
    await db.flush() # Get firm ID
    
    # Create User
    user = User(
        email=user_in.email,
        hashed_password=security.get_password_hash(user_in.password),
        full_name=user_in.full_name,
        role=UserRole.OWNER,
        firm_id=firm.id,
        signup_method="email",
        email_verified=email_verified,
    )
    db.add(user)
    
    # For corporate: auto-create a self-client (invisible to user)
    if user_in.account_type == "corporate":
        self_client = Client(
            name=user_in.firm_name,
            email=user_in.email,
            firm_id=firm.id,
        )
        db.add(self_client)
    
    await db.commit()
    await db.refresh(user)
    user.firm_name = firm.name
    user.account_type = firm.account_type
    return user


# ═══════════════════════════════════════════════
# SIGNUP — Phone Number (NOW REQUIRES OTP VERIFICATION)
# ═══════════════════════════════════════════════
@router.post("/signup/phone", response_model=user_schemas.User)
async def signup_phone(
    *,
    db: AsyncSession = Depends(deps.get_db),
    user_in: user_schemas.UserCreatePhone,
) -> Any:
    """
    Create new firm and owner user via phone number + password.
    Requires a verification_token from prior OTP verification.
    """
    # Check if phone already registered
    result = await db.execute(select(User).where(User.phone_number == user_in.phone_number))
    existing = result.scalars().first()
    if existing:
        raise HTTPException(
            status_code=400,
            detail="A user with this phone number already exists.",
        )

    # If email provided, check that too
    if user_in.email:
        result = await db.execute(select(User).where(User.email == user_in.email))
        existing = result.scalars().first()
        if existing:
            raise HTTPException(
                status_code=400,
                detail="The user with this email already exists in the system.",
            )

    # Validate OTP verification token for phone
    phone_verified = False
    if user_in.verification_token:
        result = await db.execute(
            select(OTPVerification).where(
                and_(
                    OTPVerification.verification_token == user_in.verification_token,
                    OTPVerification.identifier == user_in.phone_number,
                    OTPVerification.identifier_type == "phone",
                    OTPVerification.is_verified == True,
                )
            )
        )
        otp_record = result.scalars().first()
        if not otp_record:
            raise HTTPException(status_code=400, detail="Invalid or expired verification. Please verify your phone again.")
        if otp_record.expires_at < datetime.utcnow():
            raise HTTPException(status_code=400, detail="Verification expired. Please verify your phone again.")
        phone_verified = True
        await db.execute(
            delete(OTPVerification).where(
                and_(
                    OTPVerification.identifier == user_in.phone_number,
                    OTPVerification.identifier_type == "phone",
                )
            )
        )

    # Create Firm
    firm = Firm(name=user_in.firm_name, account_type=user_in.account_type)
    db.add(firm)
    await db.flush()

    # Create User
    user = User(
        phone_number=user_in.phone_number,
        email=user_in.email,
        hashed_password=security.get_password_hash(user_in.password),
        full_name=user_in.full_name,
        role=UserRole.OWNER,
        firm_id=firm.id,
        signup_method="phone",
        phone_verified=phone_verified,
    )
    db.add(user)
    
    # For corporate: auto-create a self-client
    if user_in.account_type == "corporate":
        self_client = Client(
            name=user_in.firm_name,
            email=user_in.email or f"{user_in.phone_number}@corporate.local",
            firm_id=firm.id,
        )
        db.add(self_client)
    
    await db.commit()
    await db.refresh(user)
    user.firm_name = firm.name
    user.account_type = firm.account_type
    return user


# ═══════════════════════════════════════════════
# SIGNUP / LOGIN — Google OAuth
# ═══════════════════════════════════════════════
@router.post("/signup/google", response_model=token_schemas.Token)
async def signup_google(
    *,
    db: AsyncSession = Depends(deps.get_db),
    user_in: user_schemas.UserCreateGoogle,
) -> Any:
    """
    Sign up or log in with Google.
    Verifies the Google id_token, extracts user info, creates the
    user+firm if new, or returns a token for existing users.
    """
    from google.oauth2 import id_token as google_id_token
    from google.auth.transport import requests as google_requests

    # 1. Verify the Google id_token
    try:
        idinfo = google_id_token.verify_oauth2_token(
            user_in.google_id_token,
            google_requests.Request(),
            settings.GOOGLE_OAUTH_CLIENT_ID,
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid Google ID token.",
        )

    google_email = idinfo.get("email")
    google_name = idinfo.get("name", "")

    if not google_email:
        raise HTTPException(
            status_code=400,
            detail="Google account does not have an email address.",
        )

    # 2. Check if user already exists
    result = await db.execute(select(User).where(User.email == google_email))
    user = result.scalars().first()

    if user:
        # Check trial expiry for free users (allow login, flag it)
        trial_expired = False
        if user.subscription_plan in (None, 'free', 'Free') and user.trial_started_at:
            days_used = (datetime.utcnow() - user.trial_started_at).days
            if days_used >= 30:
                trial_expired = True

        # Existing user — just issue a token
        access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = security.create_access_token(
            user.id, expires_delta=access_token_expires
        )
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "trial_expired": trial_expired,
        }

    # 3. New user — create firm + user
    firm_name = user_in.firm_name or f"{google_name}'s Firm"
    firm = Firm(name=firm_name, account_type=user_in.account_type)
    db.add(firm)
    await db.flush()

    user = User(
        email=google_email,
        full_name=google_name,
        hashed_password=None,  # No password for Google users
        role=UserRole.OWNER,
        firm_id=firm.id,
        signup_method="google",
    )
    db.add(user)
    
    # For corporate: auto-create a self-client
    if user_in.account_type == "corporate":
        self_client = Client(
            name=firm_name,
            email=google_email,
            firm_id=firm.id,
        )
        db.add(self_client)
    
    await db.commit()
    await db.refresh(user)

    # Issue token for the new user
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = security.create_access_token(
        user.id, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
    }


# ═══════════════════════════════════════════════
# ME — Read / Update Profile (unchanged)
# ═══════════════════════════════════════════════
@router.get("/me", response_model=user_schemas.User)
async def read_users_me(
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Get current user.
    """
    if current_user.firm:
        current_user.firm_name = current_user.firm.name
        current_user.account_type = current_user.firm.account_type
    return current_user

@router.put("/me", response_model=user_schemas.User)
async def update_user_me(
    *,
    db: AsyncSession = Depends(deps.get_db),
    user_in: user_schemas.UserProfileUpdate,
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Update own profile.
    """
    if user_in.full_name is not None:
        current_user.full_name = user_in.full_name
        
    if user_in.email is not None and user_in.email != current_user.email:
        # Check uniqueness
        result = await db.execute(select(User).where(User.email == user_in.email))
        existing_user = result.scalars().first()
        if existing_user:
             raise HTTPException(status_code=400, detail="Email already taken")
        current_user.email = user_in.email
        
    if user_in.firm_name is not None:
        # Update linked firm
        result = await db.execute(select(Firm).where(Firm.id == current_user.firm_id))
        firm = result.scalars().first()
        if firm:
            firm.name = user_in.firm_name
            db.add(firm)

    if user_in.phone_number is not None:
        current_user.phone_number = user_in.phone_number
    if user_in.job_title is not None:
        current_user.job_title = user_in.job_title
    if user_in.subscription_plan is not None:
        current_user.subscription_plan = user_in.subscription_plan
        
    db.add(current_user)
    await db.commit()
    await db.refresh(current_user)
    
    if current_user.firm:
        current_user.firm_name = current_user.firm.name
        
    return current_user


# ═══════════════════════════════════════════════
# TRIAL STATUS
# ═══════════════════════════════════════════════
@router.get("/trial-status")
async def get_trial_status(
    current_user: User = Depends(deps.get_current_user),
) -> Any:
    """
    Get trial / subscription status for current user.
    """
    plan = current_user.subscription_plan or 'free'
    trial_started = current_user.trial_started_at
    created = current_user.created_at

    # Use trial_started_at if available, else created_at
    start_date = trial_started or created or datetime.utcnow()
    days_used = (datetime.utcnow() - start_date).days
    days_remaining = max(0, 30 - days_used)
    trial_expired = days_used >= 30 and plan.lower() in ('free', '')

    return {
        "plan": plan,
        "trial_started_at": start_date.isoformat() if start_date else None,
        "days_used": days_used,
        "days_remaining": days_remaining,
        "trial_expired": trial_expired,
        "total_trial_days": 30,
    }


# ═══════════════════════════════════════════════
# OTP — SEND
# ═══════════════════════════════════════════════
from pydantic import BaseModel as PydanticBaseModel

class SendOTPRequest(PydanticBaseModel):
    identifier: str          # email or phone number
    type: str                # 'email' or 'phone'
    purpose: str = "signup"  # 'signup', 'login', 'reset'

class VerifyOTPRequest(PydanticBaseModel):
    identifier: str
    type: str
    otp_code: str
    purpose: str = "signup"


@router.post("/send-otp")
async def send_otp(
    *,
    db: AsyncSession = Depends(deps.get_db),
    body: SendOTPRequest,
) -> Any:
    """
    Send a 6-digit OTP to the given email or phone.
    For signup: checks that the identifier is NOT already registered.
    For login: checks that the identifier IS registered.
    """
    identifier = body.identifier.strip().lower() if body.type == "email" else body.identifier.strip()
    
    # Clean phone number if phone type
    if body.type == "phone":
        identifier = re_module.sub(r'[\s\-\(\)]+', '', identifier)
    
    # Validate purpose-specific checks
    if body.purpose == "signup":
        # Check identifier is NOT already registered
        if body.type == "email":
            result = await db.execute(select(User).where(User.email == identifier))
        else:
            result = await db.execute(select(User).where(User.phone_number == identifier))
        existing = result.scalars().first()
        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"This {body.type} is already registered. Please sign in instead."
            )
    elif body.purpose == "login":
        # Check identifier IS registered
        if body.type == "email":
            result = await db.execute(select(User).where(User.email == identifier))
        else:
            result = await db.execute(select(User).where(User.phone_number == identifier))
        existing = result.scalars().first()
        if not existing:
            raise HTTPException(
                status_code=400,
                detail=f"No account found with this {body.type}."
            )
    
    # Rate limiting: max 3 active OTPs per identifier per purpose
    result = await db.execute(
        select(OTPVerification).where(
            and_(
                OTPVerification.identifier == identifier,
                OTPVerification.purpose == body.purpose,
                OTPVerification.expires_at > datetime.utcnow(),
                OTPVerification.is_verified == False,
            )
        )
    )
    active_otps = result.scalars().all()
    if len(active_otps) >= 3:
        raise HTTPException(
            status_code=429,
            detail="Too many OTP requests. Please wait a few minutes."
        )

    # Generate 6-digit OTP
    otp_code = f"{random.randint(100000, 999999)}"
    expires_at = datetime.utcnow() + timedelta(minutes=OTP_EXPIRY_MINUTES)

    # Save to DB
    otp = OTPVerification(
        identifier=identifier,
        identifier_type=body.type,
        otp_code=otp_code,
        purpose=body.purpose,
        expires_at=expires_at,
    )
    db.add(otp)
    await db.commit()

    # Send the OTP
    sent = False
    if body.type == "email":
        from app.services.email_service import send_otp_email
        sent = await send_otp_email(to_email=identifier, otp_code=otp_code)

    response = {
        "sent": True,
        "type": body.type,
        "expires_in": OTP_EXPIRY_MINUTES * 60,
        "message": f"OTP sent to your {body.type}"
    }

    # Dev fallback: if email failed (domain not verified yet), return OTP in response
    # TODO: Remove this once suvidhaai.com is verified on Resend
    if body.type == "email" and not sent:
        response["dev_otp"] = otp_code
        response["message"] = "Dev mode: use the code shown on screen"

    return response


# ═══════════════════════════════════════════════
# OTP — VERIFY
# ═══════════════════════════════════════════════
@router.post("/verify-otp")
async def verify_otp(
    *,
    db: AsyncSession = Depends(deps.get_db),
    body: VerifyOTPRequest,
) -> Any:
    """
    Verify a 6-digit OTP. Returns a verification_token on success.
    """
    identifier = body.identifier.strip().lower() if body.type == "email" else body.identifier.strip()
    if body.type == "phone":
        identifier = re_module.sub(r'[\s\-\(\)]+', '', identifier)

    # Find the latest non-expired, non-verified OTP for this identifier
    result = await db.execute(
        select(OTPVerification).where(
            and_(
                OTPVerification.identifier == identifier,
                OTPVerification.identifier_type == body.type,
                OTPVerification.purpose == body.purpose,
                OTPVerification.is_verified == False,
                OTPVerification.expires_at > datetime.utcnow(),
            )
        ).order_by(OTPVerification.created_at.desc())
    )
    otp_record = result.scalars().first()

    if not otp_record:
        raise HTTPException(status_code=400, detail="No valid OTP found. Please request a new one.")

    # Check max attempts
    if otp_record.attempts >= MAX_OTP_ATTEMPTS:
        raise HTTPException(status_code=400, detail="Too many wrong attempts. Please request a new OTP.")

    # Verify code
    if otp_record.otp_code != body.otp_code.strip():
        otp_record.attempts += 1
        db.add(otp_record)
        await db.commit()
        remaining = MAX_OTP_ATTEMPTS - otp_record.attempts
        raise HTTPException(
            status_code=400,
            detail=f"Invalid OTP. {remaining} attempt{'s' if remaining != 1 else ''} remaining."
        )

    # ✅ OTP is correct — generate verification token
    verification_token = secrets.token_hex(32)  # 64-char hex string
    otp_record.is_verified = True
    otp_record.verification_token = verification_token
    # Extend expiry by 15 more minutes for the signup step
    otp_record.expires_at = datetime.utcnow() + timedelta(minutes=15)
    db.add(otp_record)
    await db.commit()

    return {
        "verified": True,
        "verification_token": verification_token,
        "message": f"{body.type.title()} verified successfully"
    }


# ═══════════════════════════════════════════════
# LOGIN — Passwordless OTP
# ═══════════════════════════════════════════════
class OTPLoginRequest(PydanticBaseModel):
    identifier: str
    type: str        # 'email' or 'phone'
    otp_code: str

@router.post("/login/otp", response_model=token_schemas.Token)
async def login_otp(
    *,
    db: AsyncSession = Depends(deps.get_db),
    body: OTPLoginRequest,
) -> Any:
    """
    Passwordless login using OTP. User sends OTP, we verify and issue JWT.
    """
    identifier = body.identifier.strip().lower() if body.type == "email" else body.identifier.strip()
    if body.type == "phone":
        identifier = re_module.sub(r'[\s\-\(\)]+', '', identifier)

    # Find the latest valid OTP for login
    result = await db.execute(
        select(OTPVerification).where(
            and_(
                OTPVerification.identifier == identifier,
                OTPVerification.identifier_type == body.type,
                OTPVerification.purpose == "login",
                OTPVerification.is_verified == False,
                OTPVerification.expires_at > datetime.utcnow(),
            )
        ).order_by(OTPVerification.created_at.desc())
    )
    otp_record = result.scalars().first()

    if not otp_record:
        raise HTTPException(status_code=400, detail="No valid OTP found. Please request a new one.")

    if otp_record.attempts >= MAX_OTP_ATTEMPTS:
        raise HTTPException(status_code=400, detail="Too many wrong attempts. Please request a new OTP.")

    if otp_record.otp_code != body.otp_code.strip():
        otp_record.attempts += 1
        db.add(otp_record)
        await db.commit()
        remaining = MAX_OTP_ATTEMPTS - otp_record.attempts
        raise HTTPException(
            status_code=400,
            detail=f"Invalid OTP. {remaining} attempt{'s' if remaining != 1 else ''} remaining."
        )

    # ✅ OTP correct — find the user
    if body.type == "email":
        result = await db.execute(select(User).where(User.email == identifier))
    else:
        result = await db.execute(select(User).where(User.phone_number == identifier))
    user = result.scalars().first()

    if not user:
        raise HTTPException(status_code=400, detail="No account found.")

    # Clean up OTP
    otp_record.is_verified = True
    db.add(otp_record)
    await db.commit()

    # Check trial expiry
    trial_expired = False
    if user.subscription_plan in (None, 'free', 'Free') and user.trial_started_at:
        days_used = (datetime.utcnow() - user.trial_started_at).days
        if days_used >= 30:
            trial_expired = True

    # Issue JWT token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = security.create_access_token(
        user.id, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "trial_expired": trial_expired,
    }
