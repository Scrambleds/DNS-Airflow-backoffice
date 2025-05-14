FROM apache/airflow:2.9.0

USER airflow
RUN pip install oracledb

ENV PYTHONPATH="/opt/airflow"

# คัดลอก requirements.txt เข้าไปใน image
COPY requirements.txt /requirements.txt

# ติดตั้ง Python packages ที่ต้องใช้
RUN pip install --no-cache-dir -r /requirements.txt