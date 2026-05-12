from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials

from addresses.schemas import AddAddressRequest, AddressResponse
from auth.security import bearer_scheme, decode_access_token
from config import postgres


router = APIRouter(prefix="/addresses", tags=["addresses"])


@router.post("", response_model=AddressResponse, status_code=status.HTTP_201_CREATED)
async def create_address(
    data: AddAddressRequest,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO addresses (user_id, street, city, apartment, extra_info)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING address_id, street, city, apartment, extra_info
            """,
            user_id,
            data.street,
            data.city,
            data.apartment,
            data.extra_info,
        )

    return AddressResponse(**dict(row))


@router.get("", response_model=list[AddressResponse])
async def list_addresses(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT address_id, street, city, apartment, extra_info FROM addresses WHERE user_id = $1",
            user_id,
        )

    return [AddressResponse(**dict(r)) for r in rows]


@router.delete("/{address_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_address(
    address_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    user_id = decode_access_token(credentials)

    pool = await postgres.get_pool()
    async with pool.acquire() as conn:
        deleted = await conn.fetchval(
            "DELETE FROM addresses WHERE address_id = $1 AND user_id = $2 RETURNING address_id",
            address_id,
            user_id,
        )

    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Address not found")
