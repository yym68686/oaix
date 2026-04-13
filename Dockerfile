FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md import_tokens.py /app/
COPY oaix_gateway /app/oaix_gateway

RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "oaix_gateway.api_server:app", "--host", "0.0.0.0", "--port", "8000"]
