# use the official slim Python image for smaller footprint
FROM python:3.10-slim

# set working directory inside container
WORKDIR /app

# install system dependencies needed for building and running
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# copy requirements and install Python dependencies
COPY ./backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy backend application code
COPY ./backend/app ./app

# copy frontend static files
COPY ./frontend ./static/frontend
COPY ./index.html ./index.html

# expose container port (can be configured via environment variable)
ENV PORT=8080
EXPOSE $PORT

# command to run the FastAPI application with uvicorn server
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
