# One image recipe for every service; CI picks the service with --build-arg SERVICE=...
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first: this layer is cached and shared by all four service images.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY services ./services

# Numeric UID so Kubernetes can verify runAsNonRoot.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
USER 10001

# Declared last so changing the service never invalidates the layers above.
ARG SERVICE
ENV SERVICE=${SERVICE}

EXPOSE 8000
CMD ["sh", "-c", "exec uvicorn services.${SERVICE}.main:app --host 0.0.0.0 --port 8000"]
