import json
import os

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.schemas import (
    LoginRequest,
    RegisterRequest,
    RegisterResponse,
    TokenResponse,
    UserResponse,
)
from auth.security import (
    bearer_scheme,
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)
from config import kafka, postgres


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=RegisterResponse)
async def register(data: RegisterRequest):
    password_hash = hash_password(data.password)

    event = {
    "type": "user.registered",
    "data": {
        "email": data.email,
        "password_hash": password_hash,
        "full_name": data.full_name,
        "phone_number": data.phone_number,
        "role": "customer",
    },
}

    producer = await kafka.get_producer()
    topic = os.environ.get("KAFKA_USER_EVENTS_TOPIC", "user-events")

    await producer.send_and_wait(
        topic,
        json.dumps(event).encode("utf-8"),
    )

    return RegisterResponse(
        status="accepted",
        message="Registration request sent to worker",
    )


@router.post("/login", response_model=TokenResponse)
async def login(data: LoginRequest):
    pool = await postgres.get_pool()

    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            """
            SELECT user_id, email, password_hash
            FROM users
            WHERE email = $1
            """,
            data.email,
        )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    if not user["password_hash"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    if not verify_password(data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    access_token = create_access_token(user["user_id"])

    return TokenResponse(access_token=access_token)


@router.get("/me", response_model=UserResponse)
async def me(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()

    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            """
            SELECT user_id, email, full_name, role, phone_number
            FROM users
            WHERE user_id = $1
            """,
            user_id,
        )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return UserResponse(
        user_id=user["user_id"],
        email=user["email"],
        full_name=user["full_name"],
        role=user["role"],
        phone_number=user["phone_number"],
    )
