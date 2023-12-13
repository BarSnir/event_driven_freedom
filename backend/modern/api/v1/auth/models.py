from pydantic import BaseModel, EmailStr
from ..customers.models import CustomerResponseModel


class SignInRequestModel(BaseModel):
    Email: str
    Password: str


class SignUpRequestModel(BaseModel):
    Email: EmailStr
    Password: str
    FirstName: str
    LastName: str


class TokenModel(BaseModel):
    access_token: str
    refresh_token: str


class CustomerAuthResponseModel(BaseModel):
    # token: TokenModel
    user: CustomerResponseModel


class AccessTokenResponseModel(BaseModel):
    access_token: str