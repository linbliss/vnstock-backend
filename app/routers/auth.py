"""Authentication router — JWT + bcrypt.

POST /login    {email, password} → {token, user}
POST /register {email, password} → {token, user}
GET  /me       Bearer token      → {id, email}
"""
import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from app.services import user_store

router = APIRouter()

SECRET_KEY  = os.environ.get("JWT_SECRET_KEY", "change-me-in-production-please")
ALGORITHM   = "HS256"
TOKEN_EXPIRE_DAYS = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def _verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


def _create_token(user_id: str) -> str:
    expire = datetime.utcnow() + timedelta(days=TOKEN_EXPIRE_DAYS)
    payload = {"sub": user_id, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    """FastAPI dependency — giải mã JWT và trả về user dict {id, email}."""
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: Optional[str] = payload.get("sub")
        if not user_id:
            raise credentials_exc
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError:
        raise credentials_exc

    user = user_store.get_user_by_id(user_id)
    if not user:
        raise credentials_exc
    return user


# ── Schemas ───────────────────────────────────────────────────────────────────

class AuthBody(BaseModel):
    email: str
    password: str


# ── Routes ───────────────────────────────────────────────────────────────────

@router.post("/register", status_code=201)
def register(body: AuthBody):
    """Đăng ký tài khoản mới.
    Hiện tại luôn cho phép đăng ký (không giới hạn số lượng user).
    """
    existing = user_store.get_user_by_email(body.email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    if len(body.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")

    hashed = _hash_password(body.password)
    user   = user_store.create_user(body.email, hashed)
    token  = _create_token(user["id"])
    return {"token": token, "user": {"id": user["id"], "email": user["email"]}}


@router.post("/login")
def login(body: AuthBody):
    user = user_store.get_user_by_email(body.email)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not _verify_password(body.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = _create_token(user["id"])
    return {"token": token, "user": {"id": user["id"], "email": user["email"]}}


@router.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return {"id": current_user["id"], "email": current_user["email"]}
