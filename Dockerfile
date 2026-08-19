# Airflow image for the flight price pipeline.
#
# Built on the official Airflow image so the scheduler, webserver, and any
# ad hoc `docker compose run` command all execute the exact same dependency
# set — the pinned Airflow version is the single source of truth.
FROM apache/airflow:2.10.5-python3.11

# Installed as the `airflow` user: pip must not write into root-owned site-packages.
USER airflow

COPY requirements.txt /requirements.txt

# --no-cache-dir keeps the image small; the constraints file is the official
# one for this exact Airflow/Python pair, so provider packages resolve to
# versions known to work with Airflow 2.10.5 instead of the newest release.
RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
        -r /requirements.txt
