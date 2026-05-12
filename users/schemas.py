from pydantic import BaseModel


class UserAdminResponse(BaseModel):
    user_id: int
    email: str
    full_name: str | None = None
    role: str
    phone_number: str | None = None


class UserSearchResponse(BaseModel):
    total: int
    results: list[UserAdminResponse]
