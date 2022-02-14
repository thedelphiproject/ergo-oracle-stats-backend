# FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
FROM python:3.10

COPY ./app /app
WORKDIR /app

RUN pip install --no-cache-dir --upgrade -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]