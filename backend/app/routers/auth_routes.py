#--------------------------------------------------------------------
# File Name:   auth_routes.py
# Description: FastAPI router with a POST endpoint "/token" that 
#              authenticates user credentials via OAuth2 and returns 
#              a JWT access token for secure authorization
# Author:      Monica Dhieu
# Date:        2025-10-14
#--------------------------------------------------------------------

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from auth import authenticate_user, create_access_token, Token, fake_users_db

router = APIRouter()

@router.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    OAuth2 token endpoint returning JWT access token after verifying credentials
    """
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.username}, expires_delta=timedelta(minutes=60))
    return {"access_token": access_token, "token_type": "bearer"}
