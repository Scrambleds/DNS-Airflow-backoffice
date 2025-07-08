FROM apache/airflow:2.9.0-python3.12
USER airflow
COPY requirements.txt /tmp/requirements.txt
#RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
RUN pip install --no-cache-dir --extra-index-url https://my-private-pypi.example.com/simple/ -r /tmp/requirements.txt

# Installing Oracle instant client
#USER root
# WORKDIR /opt/oracle
#RUN mkdir -p /opt/oracle \
#    && apt-get update \
#    && apt-get install wget \
#    && apt-get install unzip \
#    && cd /opt/oracle \
#    && wget https://download.oracle.com/otn_software/linux/instantclient/216000/instantclient-basic-linux.x64-21.6.0.0.0dbru.zip \
#    && unzip instantclient-basic-linux.x64-21.6.0.0.0dbru.zip \
#    && apt-get install libaio1 \
#    && sh -c "echo /opt/oracle/instantclient_21_6 > /etc/ld.so.conf.d/oracle-instantclient.conf" \
#    && ldconfig
#USER 1001


