from fastapi import APIRouter, Depends, HTTPException, status, Body
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from app.auth import authenticate_user, create_access_token, Token, fake_users_db
from app.auth import get_current_active_user, User

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

@router.post("/token-json", response_model=Token)
async def login_token_json(payload: dict = Body(...)):
    """JSON alternative to OAuth2 form endpoint: accepts {username, password}"""
    username = payload.get("username")
    password = payload.get("password")
    user = authenticate_user(fake_users_db, username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user.username}, expires_delta=timedelta(minutes=60))
    return {"access_token": access_token, "token_type": "bearer"}

@router.get("/ping")
def ping():
    return {"status": "ok"}

@router.get("/whoami")
def whoami(current_user: User = Depends(get_current_active_user)):
    return {"username": current_user.username}
