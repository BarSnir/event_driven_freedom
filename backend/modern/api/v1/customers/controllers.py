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

def get_single_customer(customerId: int) -> dict:
    customer = query_get(
        """
        SELECT
            *
        FROM Customers
        WHERE CustomerId=%s
        """,
        (customerId),
    )
    if len(customer) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Customer not found"
        )
    return customer[0]

def get_customer_by_email(email: str) -> list[dict]:
    customer = query_get(
        """
        SELECT
            CustomerId,
            FirstName,
            LastName,
            Password,
            Email
        FROM Customers
        WHERE Email = %s
        """,
        (email),
    )
    return customer