FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN useradd -ms /bin/bash appuser
WORKDIR /app

# install + package…
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir "filter-aggregator==1.1.3"

# create a writable logs dir and hand over /app to appuser
RUN mkdir -p /app/logs && chown -R appuser:appuser /app

USER appuser
CMD ["python", "-m", "filter_aggregator.filter"]
