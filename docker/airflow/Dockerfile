FROM apache/airflow:2.9.1-python3.9

USER root
# Install tools tambahan jika perlu (misal: git, vim)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements dan install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt