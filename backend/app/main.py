from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# import modular API routers
from app.routers import auth_routes, trip_routes, vendor_routes

app = FastAPI(
    title="NYC Train Mobility API",
    description="API backend for NYC train mobility data with JWT authentication"
)

# configure CORS middleware to allow cross-origin requests from specified origins
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8000",
    "*" # allow all origins for dev.
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # restrict access for security
    allow_credentials=True,
    allow_methods=["*"], # allow all HTTP methods
    allow_headers=["*"], # allow all headers
)

# serve frontend static files
app.mount("/static", StaticFiles(directory="frontend"), name="static")

# include routers
app.include_router(auth_routes.router, prefix="/auth", tags=["Authentication"])
app.include_router(trip_routes.router, prefix="/trips", tags=["Trips"])
app.include_router(vendor_routes.router, prefix="/vendors", tags=["Vendors"])

@app.get("/")
async def read_root():
    """
    Root endpoint to verify the API is running
    """
    return {"message": "Welcome to NYC Train Mobility API"}

# add startup and shutdown events for resource management
@app.on_event("startup")
async def startup_event():
    # initialize resources if needed
    pass

@app.on_event("shutdown")
async def shutdown_event():
    # cleanup resources if needed
    pass
