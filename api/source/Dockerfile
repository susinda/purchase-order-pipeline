FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

COPY api/source/requirements.txt /tmp/source-requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/source-requirements.txt

COPY api/source/main.py api/source/main.py
COPY requirements_docs/sample_data/ requirements_docs/sample_data/

USER 10001:10001
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "api.source.main:app", "--host", "0.0.0.0", "--port", "8000"]
