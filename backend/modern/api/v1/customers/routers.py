from fastapi import APIRouter, Depends, status
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBearer
from fastapi.responses import JSONResponse
from .models import CustomerResponseModel
from .controllers import get_all_customers, get_single_customer
from ..auth.auth_bearer import JWTBearer

router = APIRouter()


@router.get("/v1/customers", response_model=list[CustomerResponseModel])
def get_all_customers_api():
    """
    This customers get API allow you to fetch all customer data.
    """
    customers = get_all_customers()
    return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder(customers))

@router.get("/v1/customers/{customerId}",dependencies=[Depends(JWTBearer())], response_model=CustomerResponseModel)
def get_single_customer_api(customerId: int):
    """This single customer API allow you to patch single customer data."""
    customer = get_single_customer(customerId)
    return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder(customer))