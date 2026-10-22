FROM node:22-alpine AS frontend
WORKDIR /ui
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir . && useradd --uid 10001 --create-home flowtrail \
    && mkdir -p /app/data && chown -R flowtrail:flowtrail /app
COPY --from=frontend /ui/dist/ ./frontend/dist/
ENV FLOWTRAIL_DB=/app/data/flowtrail.sqlite3
USER flowtrail
EXPOSE 8000
CMD ["python", "-m", "flowtrail", "serve", "--host", "0.0.0.0", "--port", "8000"]
