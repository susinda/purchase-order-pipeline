FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

FROM base AS test
COPY api/source/requirements.txt api/source/
COPY api/target/requirements.txt api/target/
COPY integration/requirements.txt integration/
RUN python -m pip install --no-cache-dir \
    -r api/source/requirements.txt \
    -r api/target/requirements.txt \
    -r integration/requirements.txt \
    'httpx>=0.28,<1'

COPY api/auth.py api/auth.py
COPY api/source/main.py api/source/main.py
COPY api/target/database.py api/target/database.py
COPY api/target/init_db.py api/target/init_db.py
COPY api/target/main.py api/target/main.py
COPY api/target/schema.sql api/target/schema.sql
COPY integration/ integration/
COPY tests/ tests/
COPY requirements_docs/sample_data/ requirements_docs/sample_data/
RUN python -m unittest discover -s tests -v

FROM base AS runtime
ARG APP_IMAGE_TAG
ENV APP_IMAGE_TAG=${APP_IMAGE_TAG}
COPY --from=test /app/api/auth.py api/auth.py
COPY --from=test /app/api/target/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY --from=test /app/api/target/ api/target/

USER 10001:10001
EXPOSE 8001
CMD ["python", "-m", "uvicorn", "api.target.main:app", "--host", "0.0.0.0", "--port", "8001"]
