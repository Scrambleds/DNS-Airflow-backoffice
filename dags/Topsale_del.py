import configparser
# from airflow import DAG
# from airflow.decorators import dag, task, task_group  # เพิ่ม import นี้
# from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import oracledb
from pythainlp.util import thai_strftime
from airflow.utils.dates import days_ago
# import pytz
import requests
import pendulum
import os
from os import path
import base64
import io
import logging
# from dotenv import find_dotenv, load_dotenv
# from powered import powered_by_interns
from pythainlp.util import thai_strftime
from datetime import datetime
import copy
import time
# import cx_Oracle

config_file_path = 'config/Topsale.cfg'
config = configparser.ConfigParser()
config.read(config_file_path)
local = config.get("variable","tz")
local_tz = pendulum.timezone(local)
currentDateAndTime = datetime.now(tz=local_tz)
currentDate = currentDateAndTime.strftime("%Y-%m-%d") 
     
# def ConOracle():
#     try:
#         env = os.getenv('ENV', 'dev')
#         db_host = config.get(env, 'host')
#         db_port = config.get(env, 'port')
#         db_username = config.get(env, 'username')
#         db_password = config.get(env, 'password')
#         db_name = config.get(env, 'dbname')

#         # สร้าง DSN สำหรับ cx_Oracle
#         dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_name)
        
#         # สร้าง connection ด้วย cx_Oracle
#         conn = cx_Oracle.connect(
#             user=db_username,
#             password=db_password,
#             dsn=dsn
#         )
        
#         cursor = conn.cursor()
#         print(f"Connecting database {db_name} using cx_Oracle")
#         return cursor, conn
        
#     except cx_Oracle.Error as error:
#         message = f"เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB : {error}"
        
#         print("เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB:", error)
#         return message, None

# Connect DB
def ConOracle():
    try:
        env = os.getenv('ENV', 'dev')
        db_host = config.get(env, 'host')
        db_port = config.get(env, 'port')
        db_username = config.get(env, 'username')
        db_password = config.get(env, 'password')
        db_name = config.get(env, 'dbname')
        
        dsn_name = oracledb.makedsn(db_host, db_port, service_name=db_name)
        conn = oracledb.connect(user=db_username, password=db_password, dsn=dsn_name)

        cursor = conn.cursor()
        print(f"Connecting database {db_name}")
        return cursor, conn
    except oracledb.Error as error:
        message = f"เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB : {error}"
        print("เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB:", error)
        return message, None

def Get_Holidays():
    cursor, conn = ConOracle()
    try:
        cursor.execute(
            """
                SELECT * FROM XININSURE.HOLIDAY h
                WHERE h.FISCALYEAR = extract(year from sysdate)
            """
        )
        df = pd.DataFrame(
            cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
        )
        print(df)
        print(f"Get data successfully")
        return df
    except oracledb.Error as e:
        print(f"Get_holidays : {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def Check_Holiday(df):
    try:
        holiday_dates = df["HOLIDAYDATE"].dt.strftime("%Y-%m-%d")
        print(f"Holiday Dates: \n  {holiday_dates}")
        print(f"Today : {currentDate}")
        if currentDate in holiday_dates.values:
            print("Today is a holiday. Ending DAG.")
            return "Holiday_path"
            
        else:
            print("Today is not a holiday. Proceeding with work path.")
            return "Work_path"
    except Exception as e:
        print(f"Check Holiday error: {e}")
        raise e
            
# Default arguments
default_args = {
    "owner": "DCP",
    "depends_on_past": False,
    "retries":1,
    "retry_delay":timedelta(seconds=10)
}

with DAG(
    dag_id="Topsale",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    description="Topsale airflow",
    tags=["DCP"],
    start_date=datetime(2024, 4, 24, 16, 30, 0, 0, tzinfo=local_tz),
    schedule_interval="*/10 8-20 * * *",  # ทุกๆ 10 นาที จาก 8:00 ถึง 20:00 ทุกวัน
) as dag:
    
    @task.branch
    def Input(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        try: 
            df = Get_Holidays()
            result = Check_Holiday(df)
            print(result)
            message = f"Continue with {result}"
            if result == "Holiday_path":
                return result
            
            else:
                df = delete_MK()
                message += f"\nยกเลิก {len(df)} รายการ"
                print(f"\n {df.to_markdown()}")

                ti.xcom_push(key="Cancelled_work", value=df)
                return result
        except Exception as e:
            print(1)
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Input : {e}")
        finally:
            print(f"{message}")    
            
    @task
    def insert_MK():

        cursor, conn = ConOracle()
        
        try:
            query = f"""
                    SELECT * FROM TQMSALE.TOPSALE@TQMSALE
                          """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            # print(query)
            # df = pd.read_sql(sql, conn)
                
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            message = f"\nข้อมูลมีทั้งหมดรวม {len(df)} รายการ"
            print(message)
            # send_flex_notification_end(message)
            
            return {"delete_MK":df}
        except oracledb.Error as error:
            message = f'เกิดข้อผิดพลาด : {error}'
            # message = f"Fail with task {task_id} \n error : {error}"
            
        finally:
            cursor.close()
            conn.close() 
            
    @task
    def insert_MB():

        cursor, conn = ConOracle()
        
        try:
            query = f"""
                    SELECT * FROM TQMSALE.TOPSALE@TQMSALE
                          """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            # print(query)
            # df = pd.read_sql(sql, conn)
                
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            message = f"\nข้อมูลมีทั้งหมดรวม {len(df)} รายการ"
            print(message)
            # send_flex_notification_end(message)
            
            return {"delete_MK":df}
        except oracledb.Error as error:
            message = f'เกิดข้อผิดพลาด : {error}'
            # message = f"Fail with task {task_id} \n error : {error}"
            
        finally:
            cursor.close()
            conn.close() 
                   
    @task
    def insert_CS():

        cursor, conn = ConOracle()
        
        try:
            query = f"""
                    SELECT * FROM TQMSALE.TOPSALE@TQMSALE
                          """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            # print(query)
            # df = pd.read_sql(sql, conn)
                
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            message = f"\nข้อมูลมีทั้งหมดรวม {len(df)} รายการ"
            print(message)
            # send_flex_notification_end(message)
            
            return {"delete_MK":df}
        except oracledb.Error as error:
            message = f'เกิดข้อผิดพลาด : {error}'
            # message = f"Fail with task {task_id} \n error : {error}"
            
        finally:
            cursor.close()
            conn.close() 
    
    @task
    def delete_MK():

        cursor, conn = ConOracle()
        
        try:
            query = f"""
                    SELECT * FROM TQMSALE.TOPSALE@TQMSALE
                          """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            # print(query)
            # df = pd.read_sql(sql, conn)
                
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            message = f"\nข้อมูลมีทั้งหมดรวม {len(df)} รายการ"
            print(message)
            # send_flex_notification_end(message)
            
            return {"delete_MK":df}
        except oracledb.Error as error:
            message = f'เกิดข้อผิดพลาด : {error}'
            # message = f"Fail with task {task_id} \n error : {error}"
            
        finally:
            cursor.close()
            conn.close() 
            
    @task
    def delete_MB():

        cursor, conn = ConOracle()
        
        try:
            query = f"""
                    SELECT * FROM TQMSALE.TOPSALE@TQMSALE
                          """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            # print(query)
            # df = pd.read_sql(sql, conn)
                
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            message = f"\nข้อมูลมีทั้งหมดรวม {len(df)} รายการ"
            print(message)
            # send_flex_notification_end(message)
            
            return {"delete_MK":df}
        except oracledb.Error as error:
            message = f'เกิดข้อผิดพลาด : {error}'
            # message = f"Fail with task {task_id} \n error : {error}"
            
        finally:
            cursor.close()
            conn.close() 
            
    @task
    def delete_CS():

        cursor, conn = ConOracle()
        
        try:
            query = f"""
                    SELECT * FROM TQMSALE.TOPSALE@TQMSALE
                          """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            # print(query)
            # df = pd.read_sql(sql, conn)
                
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            message = f"\nข้อมูลมีทั้งหมดรวม {len(df)} รายการ"
            print(message)
            # send_flex_notification_end(message)
            
            return {"delete_MK":df}
        except oracledb.Error as error:
            message = f'เกิดข้อผิดพลาด : {error}'
            # message = f"Fail with task {task_id} \n error : {error}"
            
        finally:
            cursor.close()
            conn.close() 

    start = EmptyOperator(task_id="start", trigger_rule="none_failed_min_one_success")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    # Insert_Mk = EmptyOperator(task_id="Insert_Mk", trigger_rule="none_failed_min_one_success")
    Holiday_path = EmptyOperator(task_id="Holiday_path", trigger_rule="none_failed_min_one_success")
    Work_path = EmptyOperator(task_id="Work_path", trigger_rule="none_failed_min_one_success")
    Join_mb = EmptyOperator(task_id="Join_mb", trigger_rule="none_failed_min_one_success")
    Join_mk = EmptyOperator(task_id="Join_mk", trigger_rule="none_failed_min_one_success")
    Join_cs = EmptyOperator(task_id="Join_cs", trigger_rule="none_failed_min_one_success")
    # delete_MB_task = EmptyOperator(task_id="delete_MB_task", trigger_rule="none_failed_min_one_success")
    # Insert_MB = EmptyOperator(task_id="Insert_MB", trigger_rule="none_failed_min_one_success")
    # Insert_CS = EmptyOperator(task_id="Insert_CS", trigger_rule="none_failed_min_one_success")

    Input_task = Input()
    
    @task_group
    def Group_MK():
        delete_MK_task = delete_MK()
        insert_MK_task = insert_MK()
        delete_MK_task >> insert_MK_task
        # >> Update_x_task >> Split_qccode_task
        
    Process_MK_group = Group_MK()
    
    @task_group
    def Group_MB():
        delete_MB_task = delete_MB()
        insert_MB_task = insert_MB()
        delete_MB_task >> insert_MB_task
        # >> Update_x_task >> Split_qccode_task
        
    Process_MB_group = Group_MB()
    
    @task_group
    def Group_CS():
        delete_CS_task = delete_CS()
        insert_CS_task = insert_CS()
        delete_CS_task >> insert_CS_task
        # >> Update_x_task >> Split_qccode_task
        
    Process_CS_group = Group_CS()
    
    (
        start >> Input_task >> [Holiday_path,Work_path],
        Holiday_path >> end,
        Work_path >> [Join_mk, Join_mb, Join_cs], 
        
        Join_mk >> Process_MK_group >> end,
        
        Join_mb >> Process_MB_group >> end,
             
        Join_cs >> Process_CS_group >> end,
    )