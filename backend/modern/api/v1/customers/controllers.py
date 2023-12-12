from fastapi import HTTPException, status
from ..db.query import query_get, query_put
# from auth.provider import AuthProvider
# from user.models import UserUpdateRequestModel
from .models import CustomerResponseModel

def get_all_customers(limit: int = 10, offset: int = 0) -> list[dict]:
    customers = query_get(
        """
        SELECT
            *
        FROM Customers
        LIMIT %s OFFSET %s
        """,
        (limit, offset),
    )
    return customers