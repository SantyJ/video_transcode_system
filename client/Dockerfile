FROM python:3.12-slim
RUN apt-get update && apt-get install -y curl iputils-ping netcat-openbsd wget docker.io && rm -rf /var/lib/apt/lists/*
WORKDIR /client
COPY . .
RUN pip install requests
# Run the upload script, then keep container alive
CMD ["sh", "-c", "python smart_upload.py && tail -f /dev/null"]
