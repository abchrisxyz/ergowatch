FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8-slim

COPY requirements.txt ./

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src/main /app
