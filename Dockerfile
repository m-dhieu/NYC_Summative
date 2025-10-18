# Root Dockerfile to support platforms that expect Dockerfile at repo root
# Delegates to backend/Dockerfile while keeping context at repo root for COPY paths.

FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install requirements
COPY backend/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code and assets
COPY backend/app ./app
COPY backend/nyc_train.db ./nyc_train.db
COPY frontend ./frontend
COPY index.html ./index.html

ENV PORT=8000
EXPOSE $PORT

CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers"]
