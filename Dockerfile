FROM apache/airflow:2.9.0-python3.12

# ติดตั้ง dependencies ด้วย root ก่อน
USER root

# ติดตั้ง build tools และ Oracle Instant Client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libaio1 \
    wget \
    unzip \
    && wget https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip \
    && unzip instantclient-basiclite-linuxx64.zip -d /usr/local \
    && rm instantclient-basiclite-linuxx64.zip \
    && apt-get purge -y --auto-remove wget unzip \
    && rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/usr/local/instantclient_21_1:$LD_LIBRARY_PATH

# เปลี่ยนมาใช้ user airflow และติดตั้ง Python packages
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --extra-index-url https://my-private-pypi.example.com/simple/ -r /tmp/requirements.txt