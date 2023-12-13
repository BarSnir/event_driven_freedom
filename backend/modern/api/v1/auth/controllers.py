from datetime import datetime
from fastapi import HTTPException, status
from ..db.query import query_put
from .auth import create_access_token, create_refresh_token, get_hashed_password, verify_password
from .models import SignUpRequestModel
from ..customers.controllers import get_customer_by_email
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import status, Depends
from fastapi.responses import RedirectResponse


def register_customer(customer_model: SignUpRequestModel):
    customer = get_customer_by_email(customer_model.Email)
    if len(customer) != 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="Email already exists."
        )
    hashed_password = get_hashed_password(customer_model.Password)
    join_date = datetime.today().strftime('%Y-%m-%d')
    query_put(
        """
                INSERT INTO Customers (
                    FirstName,
                    LastName,
                    Email,
                    Password,
                    JoinDate

                ) VALUES (%s,%s,%s,%s,%s)
                """,
        (
            customer_model.FirstName,
            customer_model.LastName,
            customer_model.Email,
            hashed_password,
            join_date
            
        ),
    )
    return {
        "FirstName": customer_model.FirstName,
        "FirstName": customer_model.LastName,
        "Email": customer_model.Email,
        # Do not return the password hash or any sensitive information
    }


def signin_customer(form_data: OAuth2PasswordRequestForm = Depends()):
    customer = get_customer_by_email(form_data.Email)
    print(customer)
    if customer is None:
        return RedirectResponse(url="/v1/login", status_code=status.HTTP_401_UNAUTHORIZED)
    hashed_password = customer[0]["Password"]
    if not verify_password(form_data.Password, hashed_password):
        raise Exception(status_code=status.HTTP_400_BAD_REQUEST,
                        detail="incorrect email or password")
    tokens = {"access_token": create_access_token(
        str(customer[0]['CustomerId'])), "refresh_token": create_refresh_token(str(customer[0]['CustomerId']))}
    return tokens, customer