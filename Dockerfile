# Use official Python image
FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y ffmpeg curl iputils-ping netcat-openbsd wget procps docker.io psmisc bc && rm -rf /var/lib/apt/lists/*

# Set working directory in container
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only container backend scripts
COPY container_scripts/ .

# Expose FastAPI's port
EXPOSE 5000

# Start FastAPI server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
