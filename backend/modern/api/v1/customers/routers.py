from fastapi import APIRouter, Depends, status
from fastapi.encoders import jsonable_encoder
from fastapi.security import HTTPBearer
from fastapi.responses import JSONResponse
from .models import CustomerResponseModel
from .controllers import get_all_customers

router = APIRouter()


@router.get("/v1/customers", response_model=list[CustomerResponseModel])
def get_all_customers_api():
    """
    This customers get API allow you to fetch all customer data.
    """
    customers = get_all_customers()
    return JSONResponse(status_code=status.HTTP_200_OK, content=jsonable_encoder(customers))