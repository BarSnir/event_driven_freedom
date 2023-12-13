from typing import Union
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from .v1.customers.routers import router as customers_router
from .v1.auth.routers import router as auth_router

app = FastAPI()

app.include_router(customers_router)
app.include_router(auth_router)

origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:4000",
    "http://localhost:19006",
    # Add your frontend URL here...
]

# Set middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def healthCheck():
    return {"Server": "Live"}