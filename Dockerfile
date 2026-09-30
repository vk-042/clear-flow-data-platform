FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless tini \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts
COPY .streamlit ./.streamlit
RUN pip install --no-cache-dir '.[spark]'
RUN useradd --create-home --uid 10001 app && mkdir -p /app/data /home/app/.ivy2 \
    && chown -R app:app /app /home/app
USER app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "clearflow.cli", "stats"]
