# Use official Python image
FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y curl iputils-ping netcat-openbsd wget && rm -rf /var/lib/apt/lists/*

# Set working directory in container
WORKDIR /app

# Copy scripts to container
COPY central_server.py .

# install modules
RUN pip install requests fastapi uvicorn

# Expose FastAPI's port
EXPOSE 8005

# Start FastAPI server
CMD ["uvicorn", "central_server:app", "--host", "0.0.0.0", "--port", "8005"]