# GraETL - single-container image (API + console).
#
# No Node stage: the console is plain ES modules served from the package.
#
#   docker build -t graetl .
#   docker run -p 8777:8777 -v "$PWD/pipelines:/app/pipelines" graetl
#
# The pipelines volume is the whole portable state: code, config, data, state.db
# and etl.db all live there.

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    GRAETL_ROOT=/app \
    GRAETL_HOST=0.0.0.0

WORKDIR /app
COPY pyproject.toml README.md ./
COPY backend/ ./backend/
RUN pip install --no-cache-dir -e ".[data]"

COPY graetl.toml ./

VOLUME ["/app/pipelines"]
EXPOSE 8777
CMD ["graetl", "serve", "--host", "0.0.0.0"]
