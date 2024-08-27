FROM quay.io/astronomer/astro-runtime:11.8.0

RUN python -m venv dbtt_venv && source dbtt_venv/bin/activate && \
    pip install --no-cache-dir dbt-postgres && deactivate