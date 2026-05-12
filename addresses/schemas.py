from pydantic import BaseModel


class AddAddressRequest(BaseModel):
    street: str
    city: str
    apartment: str | None = None
    extra_info: str | None = None


class AddressResponse(BaseModel):
    address_id: int
    street: str
    city: str
    apartment: str | None = None
    extra_info: str | None = None
