from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials

from auth.security import bearer_scheme, decode_access_token
from users.schemas import UserAdminResponse, UserSearchResponse
from config import elasticsearch, postgres


router = APIRouter(prefix="/users", tags=["users"])


async def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        role = await conn.fetchval(
            "SELECT role FROM users WHERE user_id = $1", user_id
        )

    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )

    return user_id


@router.get("/search", response_model=UserSearchResponse)
async def search_users(
    q: str,
    size: int = 10,
    _: int = Depends(require_admin),
):
    should_clauses = [
        {
            "multi_match": {
                "query": q,
                "fields": ["email^3", "full_name^2", "role", "phone_number"],
                "fuzziness": "AUTO",
            }
        }
    ]

    if q.isdigit():
        should_clauses.append({"term": {"user_id": int(q)}})

    body = {
        "size": size,
        "query": {"bool": {"should": should_clauses, "minimum_should_match": 1}},
    }

    es = elasticsearch.get_client()
    resp = await es.search(index="users", body=body, ignore_unavailable=True)
    hits = resp["hits"]["hits"]
    total = resp["hits"]["total"]["value"]

    results = [
        UserAdminResponse(
            user_id=h["_source"]["user_id"],
            email=h["_source"]["email"],
            full_name=h["_source"].get("full_name"),
            role=h["_source"]["role"],
            phone_number=h["_source"].get("phone_number"),
        )
        for h in hits
    ]

    return UserSearchResponse(total=total, results=results)
