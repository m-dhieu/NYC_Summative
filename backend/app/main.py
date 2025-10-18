from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

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
    # Allow overriding DB path via environment variable (useful in containers)
    try:
        from app.database.connection import set_database_path, DatabaseConnection
        db_path = os.getenv("DB_PATH", "nyc_train.db")
        set_database_path(db_path)

        # Initialize schema if DB is missing/empty
        if not os.path.exists(db_path) or os.path.getsize(db_path) == 0:
            schema_path = os.path.join(os.path.dirname(__file__), "database", "schema.sql")
            if os.path.exists(schema_path):
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema_sql = f.read()
                DatabaseConnection(db_path).execute_script(schema_sql)
    except Exception as e:
        # Non-fatal; API can still start, but DB ops may fail until fixed
        print(f"Startup DB init skipped/failed: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    # cleanup resources if needed
    pass
