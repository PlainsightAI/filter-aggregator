# openfilter-base = python:3.11-slim + all outstanding Debian security patches (rebuilt
# weekly). It provides the PYTHONDONTWRITEBYTECODE/PYTHONUNBUFFERED env, the appuser account,
# and /app (WORKDIR) + /app/logs — so none of that is repeated here.
FROM plainsightai/openfilter-base:py3.11

# install + package…
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir "filter-aggregator==1.1.3"

USER appuser
CMD ["python", "-m", "filter_aggregator.filter"]
