from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from .models import BaseModel, CustomerAuthResponseModel, SignInRequestModel, SignUpRequestModel
from ..customers.models import CustomerSignupResponseModel
from .controllers import register_customer, signin_customer
from .auth import create_access_token, create_refresh_token
from .auth_bearer import JWTBearer

router = APIRouter()

@router.post("/v1/signup", response_model=CustomerSignupResponseModel)
def signup_api(customer_details: SignUpRequestModel):
    """
    This sign-up API allow you to register your account, and return access token.
    """
    customer = register_customer(customer_details)
    # access_token = create_access_token(customer_details.email)
    # refresh_token = create_refresh_token(customer_details.email)
    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content=jsonable_encoder(
            {
                # "token": {"access_token": access_token, "refresh_token": refresh_token},
                "customer": customer,
            }
        ),
    )

@router.post("/v1/signin", response_model=CustomerAuthResponseModel)
def signin_api(user_details: SignInRequestModel):
    """
    This sign-in API allow you to obtain your access token.
    """
    tokens, customer = signin_customer(user_details)
    # access_token = JWTBearer.verify_jwt(user["email"])
    # refresh_token = auth_handler.encode_refresh_token(user["email"])
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=jsonable_encoder(
            {
                "token": {"access_token": tokens["access_token"], "refresh_token": tokens["refresh_token"]},
                "customer": customer,
            }
        ),
    )