FROM python:3.12-slim
WORKDIR /app
COPY monitor.py .
RUN pip install requests docker
CMD ["python", "monitor.py"]