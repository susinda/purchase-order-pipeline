FROM python:3.12-slim

ARG APP_IMAGE_TAG
ENV APP_IMAGE_TAG=${APP_IMAGE_TAG} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY auth.py main.py api/source/
COPY sample_data/ api/source/sample_data/

USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "api.source.main:app", "--host", "0.0.0.0", "--port", "8000"]
