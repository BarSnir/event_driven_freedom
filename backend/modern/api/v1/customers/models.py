from pydantic import BaseModel, EmailStr, validator, constr
from typing import Optional
from datetime import datetime


class CustomerUpdateRequestModel(BaseModel):
    CustomerId: int
    Email: EmailStr 
    Password: constr 
    FirstName: str 
    LastName: str 

    # @validator("password")
    # def empty_str_to_none(cls, v):
    #     if v == "":
    #         return None
    #     return v


class CustomerResponseModel(BaseModel):
    CustomerId: int
    Email: EmailStr 
    FirstName: str 
    LastName: str 
    CustomerTypeId: int 
    CustomerTypeText: str 
    ProfileImage: str 
    IsSuspended: int 
    SuspendedReasonId: int 
    SuspendedReasonText: str 
    AuthTypeId: int 
    JoinDate: datetime 
    
