FROM apache/airflow:2.8.1

# คัดลอก requirements.txt เข้าไปใน image
COPY requirements.txt /requirements.txt

# ติดตั้ง Python packages ที่ต้องใช้
RUN pip install --no-cache-dir -r /requirements.txt