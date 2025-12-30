# backend/Dockerfile
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies (needed for some Python packages like psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the backend code
COPY . .

# Expose the port your server runs on (FastAPI/Uvicorn usually 8000)
EXPOSE 8000

# Command to run the server
# Assuming server.py is in applications/server.py and uses uvicorn internally or directly
CMD ["python", "applications/server.py"]