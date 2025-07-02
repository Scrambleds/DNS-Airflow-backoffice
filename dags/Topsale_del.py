import configparser
from airflow import DAG
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import oracledb
from pythainlp.util import thai_strftime
from airflow.utils.dates import days_ago
import requests
import pendulum
import os
from os import path
import base64
import io
import logging
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
# yesterdatDate = currentDate - 1
yesterdayDate = (currentDateAndTime - timedelta(days=1)).strftime("%Y-%m-%d")
get_dates = config.get('variable', 'get_dates')
get_gates_insert = config.get('variable', 'get_gates_insert')

now = datetime.now(local_tz)
# start_date = now.replace(hour=10, minute=0, second=0, microsecond=0, tzinfo=local_tz)
start_date = now.replace(hour=8, minute=30, second=0, microsecond=0, tzinfo=local_tz)
# ถ้าปัจจุบันยังไม่ถึง 8:30 น. ให้ใช้ 8:30 น. ของวันก่อนหน้า
if now < start_date:
    start_date = start_date - timedelta(days=1)

# def ConOracle_TQMSALE():
#     try:
#         env = os.getenv('ENV', 'tqmsale_preprod')
#         db_host = config.get(env, 'host_tqmsale')
#         db_port = config.get(env, 'port_tqmsale')
#         db_username = config.get(env, 'username_tqmsale')
#         db_password = config.get(env, 'password_tqmsale')
#         db_name = config.get(env, 'dbname_tqmsale')

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
def ConOracle_TQMSALE():
    try:
        env = os.getenv('ENV', 'tqmsale_preprod')
        db_host = config.get(env, 'host_tqmsale')
        db_port = config.get(env, 'port_tqmsale')
        db_username = config.get(env, 'username_tqmsale')
        db_password = config.get(env, 'password_tqmsale')
        db_name = config.get(env, 'dbname_tqmsale')
        
        dsn_name = oracledb.makedsn(db_host, db_port, service_name=db_name)
        conn = oracledb.connect(user=db_username, password=db_password, dsn=dsn_name)

        cursor = conn.cursor()
        print(f"Connecting database {db_name}")
        return cursor, conn
    except oracledb.Error as error:
        message = f"เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB : {error}"
        print("เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB:", error)
        return message, None
     
def ConOracle_Oracle():
    try:
        env = os.getenv('ENV', 'xininsure_preprod')
        db_host = config.get(env, 'host_xininsure')
        db_port = config.get(env, 'port_xininsure')
        db_username = config.get(env, 'username_xininsure')
        db_password = config.get(env, 'password_xininsure')
        db_name = config.get(env, 'dbname_xininsure')
        
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
    cursor, conn = ConOracle_Oracle()
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
        formatted_table = df.to_markdown(index=False)
        print(f"\n{formatted_table}")
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
        # print(f"Holiday Dates: \n  {holiday_dates}")
        formatted_table = holiday_dates.to_markdown(index=False)
        print(f"\n{formatted_table}")
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
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}

with DAG(
    dag_id="Topsale",
    default_args=default_args,
    catchup=False,
    # max_active_runs=1,
    description="Topsale airflow",
    tags=["DCP"],
    # start_date=datetime(2024, 4, 24, 16, 30, 0, 0, tzinfo=local_tz),
    start_date=start_date,  # ใช้ start_date ที่คำนวณแล้ว
    schedule_interval="*/10 8-19 * * *",
    # schedule_interval="*/10 8-19 * * *"
) as dag:
    
    @task.branch
    def check_holiday(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id}, try_number {try_number}"
        print(f"{message}")
        
        try: 
            df = Get_Holidays()
            result = Check_Holiday(df)
            print(result)
            message = f"Continue with {result}"
            if result == "Holiday_path":
                return "Holiday_path"
            else:
                # df_mk = delete_MK()
                # message += f"\nTEAM MK ลบไปทั้งหมด {len(df_mk)} รายการ"
                # print(f"\n {df_mk.to_markdown()}")
                
                # df_mb = delete_MB()
                # message += f"\nTEAM MK ลบไปทั้งหมด {len(df_mb)} รายการ"
                # print(f"\n {df_mb.to_markdown()}")
                
                # df_cs = delete_CS()
                # message += f"\nTEAM MK ลบไปทั้งหมด {len(df_cs)} รายการ"
                # print(f"\n {df_cs.to_markdown()}")

                # ti.xcom_push(key="df_mk", value=df_mk)
                # ti.xcom_push(key="df_mb", value=df_mb)    
                # ti.xcom_push(key="df_cs", value=df_cs)
                
                return "Work_path"
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"check_holiday : {e}")
        finally:
            print(f"{message}")    
            
    @task
    def select_MK():
        cursor, conn = ConOracle_Oracle()
        try:
            query = f"""
                        WITH BASE AS (
                        SELECT  
                                F.STAFFID,
                                F.STAFFCODE,
                                F.STAFFNAME,
                                D.ANALYSISCODE,
                                COUNT(*) AS TOTALSALE_MT,
                                0 AS TOTALSALE_NONMT,
                                COUNT(*) AS TOTALSALE,
                                SUM((S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) 
                                    * DECODE(S.PAYMENTSTATUS, 'Y', 1, 'P', 1, 0)) AS PAIDAMOUNT,
                                MAX(S.CREATEDATETIME) AS LASTESTSALE,
                                SUM(S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) AS TOTAL_MT,
                                0 AS TOTAL_NONMT,
                                SUM(S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) AS TOTAL
                        FROM XININSURE.SALE S
                        JOIN XININSURE.STAFF F ON S.STAFFID = F.STAFFID
                        JOIN XININSURE.DEPARTMENT D ON S.STAFFDEPARTMENTID = D.DEPARTMENTID
                        WHERE {get_dates}
                            AND S.SALESTATUS IN ('O')
                            AND S.PLATEID IS NOT NULL
                            AND D.DEPARTMENTGROUP LIKE 'MK%'
                            AND (D.ANALYSISCODE LIKE '01:กลุ่มงานรถใหม่%' 
                                OR D.ANALYSISCODE LIKE '02:กลุ่มงานรถใหม่%' 
                                OR D.ANALYSISCODE LIKE '03:กลุ่มงานพิเศษ%' 
                                OR D.ANALYSISCODE LIKE '04:กลุ่มงานรถเก่า%')
                        GROUP BY F.STAFFID, F.STAFFCODE, F.STAFFNAME, D.ANALYSISCODE
                        ),
                        TGD_DATA AS (
                        SELECT STAFFID, 
                                TRUNC(CASE WHEN WORKDAY = 0 THEN NULL ELSE TARGET / WORKDAY END, 2) AS TGD
                        FROM XININSURE.SALETARGET
                        WHERE PERIODID = TO_CHAR({get_gates_insert}, 'YYYYMM')
                        ),
                        RANKED_BY_GROUP AS (
                        SELECT 
                            'MK' AS TEAM,
                            B.ANALYSISCODE AS TEAMSUB,
                            B.STAFFCODE,
                            B.STAFFNAME,
                            (SELECT DEPARTMENTCODE
                            FROM XININSURE.DEPARTMENT
                            WHERE DEPARTMENTID = (SELECT DEPARTMENTID FROM STAFF WHERE STAFFCODE = B.STAFFCODE)
                            AND ROWNUM = 1) AS DEPARTMENTCODE,
                            B.PAIDAMOUNT,
                            B.TOTALSALE_MT,
                            B.TOTALSALE_NONMT,
                            B.TOTALSALE,
                            B.LASTESTSALE,
                            T.TGD,
                            CASE 
                            WHEN T.TGD IS NOT NULL AND T.TGD != 0 THEN TRUNC((B.PAIDAMOUNT / T.TGD) * 100, 2)
                            ELSE 0 
                            END AS TGD_PERCENT,
                            B.TOTAL_MT,
                            B.TOTAL_NONMT,
                            B.TOTAL,
                            ROW_NUMBER() OVER (PARTITION BY B.ANALYSISCODE ORDER BY B.TOTAL DESC) AS SEQUENCE
                        FROM BASE B
                        LEFT JOIN TGD_DATA T ON B.STAFFID = T.STAFFID
                        ),
                        RANKED_ALL_GROUP AS (
                        SELECT 
                            'MK' AS TEAM,
                            'กลุ่มงานทั้งหมด' AS TEAMSUB,
                            B.STAFFCODE,
                            B.STAFFNAME,
                            (SELECT DEPARTMENTCODE
                            FROM XININSURE.DEPARTMENT
                            WHERE DEPARTMENTID = (SELECT DEPARTMENTID FROM STAFF WHERE STAFFCODE = B.STAFFCODE)
                            AND ROWNUM = 1) AS DEPARTMENTCODE,
                            B.PAIDAMOUNT,
                            B.TOTALSALE_MT,
                            B.TOTALSALE_NONMT,
                            B.TOTALSALE,
                            B.LASTESTSALE,
                            T.TGD,
                            CASE 
                            WHEN T.TGD IS NOT NULL AND T.TGD != 0 THEN TRUNC((B.PAIDAMOUNT / T.TGD) * 100, 2)
                            ELSE 0 
                            END AS TGD_PERCENT,
                            B.TOTAL_MT,
                            B.TOTAL_NONMT,
                            B.TOTAL,
                            ROW_NUMBER() OVER (ORDER BY B.TOTAL DESC) AS SEQUENCE
                        FROM BASE B
                        LEFT JOIN TGD_DATA T ON B.STAFFID = T.STAFFID
                        )
                        SELECT * FROM (
                        SELECT * FROM RANKED_BY_GROUP WHERE SEQUENCE <= 5
                        UNION ALL
                        SELECT * FROM RANKED_ALL_GROUP WHERE SEQUENCE <= 5
                        )
                        ORDER BY TEAMSUB, TOTAL_NONMT DESC, TOTAL DESC
                        """
                
            # Execute the query
            cursor.execute(query)
            
            df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[desc[0] for desc in cursor.description]
            )
            
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Select data successfully")
            print(f"Total records: {len(df)}")
            
            conn.commit() 
            
            return {"select_MK": df}
            
        except oracledb.Error as error:
            conn.rollback()  
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return {"select_MK": "error", "message": message}
        finally:
            cursor.close()
            conn.close()
    
    @task
    def select_MB():
        cursor, conn = ConOracle_Oracle()
        try:
            query = f"""
                        WITH BASE AS (
                        SELECT  
                                F.STAFFID,
                                F.STAFFCODE,
                                F.STAFFNAME,
                                D.DEPARTMENTGROUPSUB,
                                COUNT(*) AS TOTALSALE_MT,
                                0 AS TOTALSALE_NONMT,
                                COUNT(*) AS TOTALSALE,
                                SUM((S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) 
                                    * DECODE(S.PAYMENTSTATUS, 'Y', 1, 'P', 1, 0)) AS PAIDAMOUNT,
                                MAX(S.CREATEDATETIME) AS LASTESTSALE,
                                SUM(S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) AS TOTAL_MT,
                                0 AS TOTAL_NONMT,
                                SUM(S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) AS TOTAL
                        FROM XININSURE.SALE S
                        JOIN XININSURE.STAFF F ON S.STAFFID = F.STAFFID
                        JOIN XININSURE.DEPARTMENT D ON S.STAFFDEPARTMENTID = D.DEPARTMENTID
                        WHERE {get_dates}
                            AND S.SALESTATUS IN ('O')
                            AND S.PLATEID IS NOT NULL
                            AND D.DEPARTMENTGROUP LIKE 'MB%'
                            AND (
                            D.DEPARTMENTGROUPSUB = 'MB_BASS' OR 
                            D.DEPARTMENTGROUPSUB = 'MB_LEK' OR
                            D.DEPARTMENTGROUPSUB = 'MB_OHH' OR
                            D.DEPARTMENTGROUPSUB = 'MB_OHM' OR
                            D.DEPARTMENTGROUPSUB = 'MB_TON' 
                            )
                        GROUP BY F.STAFFID, F.STAFFCODE, F.STAFFNAME, D.DEPARTMENTGROUPSUB
                        ),
                        TGD_DATA AS (
                        SELECT STAFFID, 
                                TRUNC(CASE WHEN WORKDAY = 0 THEN NULL ELSE TARGET / WORKDAY END, 2) AS TGD
                        FROM XININSURE.SALETARGET
                        WHERE PERIODID = TO_CHAR({get_gates_insert}, 'YYYYMM')
                        ),
                        RANKED_BY_GROUP AS (
                        SELECT 
                            'MB' AS TEAM,
                            B.DEPARTMENTGROUPSUB AS TEAMSUB,
                            B.STAFFCODE,
                            B.STAFFNAME,
                            (SELECT DEPARTMENTCODE
                            FROM XININSURE.DEPARTMENT
                            WHERE DEPARTMENTID = (SELECT DEPARTMENTID FROM STAFF WHERE STAFFCODE = B.STAFFCODE)
                            AND ROWNUM = 1) AS DEPARTMENTCODE,
                            B.PAIDAMOUNT,
                            B.TOTALSALE_MT,
                            B.TOTALSALE_NONMT,
                            B.TOTALSALE,
                            B.LASTESTSALE,
                            T.TGD,
                            CASE 
                            WHEN T.TGD IS NOT NULL AND T.TGD != 0 THEN TRUNC((B.PAIDAMOUNT / T.TGD) * 100, 2)
                            ELSE 0 
                            END AS TGD_PERCENT,
                            B.TOTAL_MT,
                            B.TOTAL_NONMT,
                            B.TOTAL,
                            ROW_NUMBER() OVER (PARTITION BY B.DEPARTMENTGROUPSUB ORDER BY B.TOTAL DESC) AS SEQUENCE
                        FROM BASE B
                        LEFT JOIN TGD_DATA T ON B.STAFFID = T.STAFFID
                        ),
                        RANKED_ALL_GROUP AS (
                        SELECT 
                            'MB' AS TEAM,
                            'ALL MB' AS TEAMSUB,
                            B.STAFFCODE,
                            B.STAFFNAME,
                            (SELECT DEPARTMENTCODE
                            FROM XININSURE.DEPARTMENT
                            WHERE DEPARTMENTID = (SELECT DEPARTMENTID FROM STAFF WHERE STAFFCODE = B.STAFFCODE)
                            AND ROWNUM = 1) AS DEPARTMENTCODE,
                            B.PAIDAMOUNT,
                            B.TOTALSALE_MT,
                            B.TOTALSALE_NONMT,
                            B.TOTALSALE,
                            B.LASTESTSALE,
                            T.TGD,
                            CASE 
                                WHEN T.TGD IS NOT NULL AND T.TGD != 0 THEN TRUNC((B.PAIDAMOUNT / T.TGD) * 100, 2)
                                ELSE 0 
                            END AS TGD_PERCENT,
                            B.TOTAL_MT,
                            B.TOTAL_NONMT,
                            B.TOTAL,
                            ROW_NUMBER() OVER (ORDER BY B.TOTAL DESC) AS SEQUENCE
                        FROM BASE B
                        LEFT JOIN TGD_DATA T ON B.STAFFID = T.STAFFID
                        )
                        SELECT * FROM (
                        SELECT * FROM RANKED_BY_GROUP WHERE SEQUENCE <= 5
                        UNION ALL
                        SELECT * FROM RANKED_ALL_GROUP WHERE SEQUENCE <= 5
                        )
                        ORDER BY TEAMSUB, TOTAL_NONMT DESC, TOTAL DESC
                        """
                
            # Execute the query
            cursor.execute(query)
            
            df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[desc[0] for desc in cursor.description]
            )
            
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Select data successfully")
            print(f"Total records: {len(df)}")
            
            conn.commit() 
            
            return {"select_MB": df}
            
        except oracledb.Error as error:
            conn.rollback()  
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return {"select_MB": "error", "message": message}
        finally:
            cursor.close()
            conn.close()
    
    @task
    def select_CS():
        cursor, conn = ConOracle_Oracle()
        try:
            query = f"""
                      WITH BASE AS (
                                    SELECT  
                                            F.STAFFID,
                                            F.STAFFCODE,
                                            F.STAFFNAME,
                                            D.ANALYSISCODE,
                                            COUNT(CASE WHEN S.PLATEID IS NOT NULL THEN 1 END) AS TOTALSALE_MT,
                                            COUNT(CASE WHEN S.PLATEID IS NULL THEN 1 END) AS TOTALSALE_NONMT,
                                            COUNT(*) AS TOTALSALE,
                                            SUM((S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) 
                                                * DECODE(S.PAYMENTSTATUS, 'Y', 1, 'P', 1, 0)) AS PAIDAMOUNT,
                                            MAX(S.CREATEDATETIME) AS LASTESTSALE,
                                            SUM(CASE WHEN S.PLATEID IS NOT NULL 
                                                    THEN (S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) 
                                                    ELSE 0 
                                                END) AS TOTAL_MT,
                                            SUM(CASE WHEN S.PLATEID IS NULL 
                                                    THEN (S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) 
                                                    ELSE 0 
                                                END) AS TOTAL_NONMT,
                                            SUM(S.NETAMOUNT + NVL(S.PRBAMOUNT, 0) + NVL(S.PRBVAT, 0) + NVL(S.PRBDUTY, 0)) AS TOTAL
                                    FROM XININSURE.SALE S
                                    JOIN XININSURE.STAFF F ON S.STAFFID = F.STAFFID
                                    JOIN XININSURE.DEPARTMENT D ON S.STAFFDEPARTMENTID = D.DEPARTMENTID
                                    WHERE {get_dates}
                                        AND S.SALESTATUS IN ('O')
                                        AND D.DEPARTMENTGROUP LIKE 'CS%'
                                    GROUP BY F.STAFFID, F.STAFFCODE, F.STAFFNAME, D.ANALYSISCODE
                                    ),
                                    TGD_DATA AS (
                                    SELECT STAFFID, 
                                            TRUNC(CASE WHEN WORKDAY = 0 THEN NULL ELSE TARGET / WORKDAY END, 2) AS TGD
                                    FROM XININSURE.SALETARGET
                                    WHERE PERIODID = TO_CHAR({get_gates_insert}, 'YYYYMM')
                                    ),
                                    RANKED_BY_GROUP AS (
                                    SELECT 
                                        'CS' AS TEAM,
                                        B.ANALYSISCODE AS TEAMSUB,
                                        B.STAFFCODE,
                                        B.STAFFNAME,
                                        (SELECT DEPARTMENTCODE
                                        FROM XININSURE.DEPARTMENT
                                        WHERE DEPARTMENTID = (SELECT DEPARTMENTID FROM STAFF WHERE STAFFCODE = B.STAFFCODE)
                                        AND ROWNUM = 1) AS DEPARTMENTCODE,
                                        B.PAIDAMOUNT,
                                        B.TOTALSALE_MT,
                                        B.TOTALSALE_NONMT,
                                        B.TOTALSALE,
                                        B.LASTESTSALE,
                                        T.TGD,
                                        CASE 
                                        WHEN T.TGD IS NOT NULL AND T.TGD != 0 THEN TRUNC((B.PAIDAMOUNT / T.TGD) * 100, 2)
                                        ELSE 0 
                                        END AS TGD_PERCENT,
                                        B.TOTAL_MT,
                                        B.TOTAL_NONMT,
                                        B.TOTAL,
                                        ROW_NUMBER() OVER (PARTITION BY B.ANALYSISCODE ORDER BY B.TOTAL DESC) AS SEQUENCE
                                    FROM BASE B
                                    LEFT JOIN TGD_DATA T ON B.STAFFID = T.STAFFID
                                    )
                                    SELECT * 
                                    FROM RANKED_BY_GROUP 
                                    WHERE SEQUENCE <= 5
                                    ORDER BY TEAMSUB, TOTAL_NONMT DESC, TOTAL DESC
                        """
                
            # Execute the query
            cursor.execute(query)
            
            df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[desc[0] for desc in cursor.description]
            )
            
            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Select data successfully")
            print(f"Total records: {len(df)}")
            
            conn.commit()
            
            return {"select_CS": df}
            
        except oracledb.Error as error:
            conn.rollback()  
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return {"select_CS": "error", "message": message}
        finally:
            cursor.close()
            conn.close()
            
    @task
    def insert_MK(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="process_mk_group.select_MK", key="return_value")

        df = result.get("select_MK") if isinstance(result, dict) else None
        cursor, conn = ConOracle_TQMSALE()
        if cursor is None or conn is None:
            return None

        try:
            if df is None or df.empty:
                print("DataFrame is empty. Exit task.")
                return df
            else:
                insert_action_query = """
                INSERT INTO TQMSALE.TOPSALE(
                    TEAM,
                    TEAMSUB,
                    STAFFCODE,
                    STAFFNAME,
                    DEPARTMENTCODE,
                    PAIDAMOUNT,
                    TOTALSALE_MT,
                    TOTALSALE_NONMT,
                    TOTALSALE,
                    LASTESTSALE,
                    TGD,
                    TGD_PERCENT,
                    TOTAL_MT,
                    TOTAL_NONMT,
                    TOTAL,
                    SEQUENCE
                ) VALUES (
                    :TEAM,
                    :TEAMSUB,
                    :STAFFCODE,
                    :STAFFNAME,
                    :DEPARTMENTCODE,
                    :PAIDAMOUNT,
                    :TOTALSALE_MT,
                    :TOTALSALE_NONMT,
                    :TOTALSALE,
                    :LASTESTSALE,
                    :TGD,
                    :TGD_PERCENT,
                    :TOTAL_MT,
                    :TOTAL_NONMT,
                    :TOTAL,
                    :SEQUENCE
                )
                """
                i = 0
                for index, row in df.iterrows():
                    cursor.execute(insert_action_query, {
                        "TEAM": row["TEAM"],
                        "TEAMSUB": row["TEAMSUB"],
                        "STAFFCODE": row["STAFFCODE"],
                        "STAFFNAME": row["STAFFNAME"],
                        "DEPARTMENTCODE": row["DEPARTMENTCODE"],
                        "PAIDAMOUNT": row["PAIDAMOUNT"],
                        "TOTALSALE_MT": row["TOTALSALE_MT"],
                        "TOTALSALE_NONMT": row["TOTALSALE_NONMT"],
                        "TOTALSALE": row["TOTALSALE"],
                        "LASTESTSALE": row["LASTESTSALE"],
                        "TGD": row["TGD"],
                        "TGD_PERCENT": row["TGD_PERCENT"],
                        "TOTAL_MT": row["TOTAL_MT"],
                        "TOTAL_NONMT": row["TOTAL_NONMT"],
                        "TOTAL": row["TOTAL"],
                        "SEQUENCE": row["SEQUENCE"]
                    })
                    print(f"Insert row {i+1}: STAFFCODE={row['STAFFCODE']}, TEAMSUB={row['TEAMSUB']}")
                    i += 1

                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
                conn.commit()
                return df

        except oracledb.Error as error:
            print(error)
            conn.rollback()
            return None

        finally:
            cursor.close()
            conn.close()
        
    @task
    def insert_MB(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="process_mb_group.select_MB", key="return_value")

        df = result.get("select_MB") if isinstance(result, dict) else None
        cursor, conn = ConOracle_TQMSALE()
        if cursor is None or conn is None:
            return None

        try:
            if df is None or df.empty:
                print("DataFrame is empty. Exit task.")
                return df 
            else:
                insert_action_query = """
                INSERT INTO TQMSALE.TOPSALE(
                    TEAM,
                    TEAMSUB,
                    STAFFCODE,
                    STAFFNAME,
                    DEPARTMENTCODE,
                    PAIDAMOUNT,
                    TOTALSALE_MT,
                    TOTALSALE_NONMT,
                    TOTALSALE,
                    LASTESTSALE,
                    TGD,
                    TGD_PERCENT,
                    TOTAL_MT,
                    TOTAL_NONMT,
                    TOTAL,
                    SEQUENCE
                ) VALUES (
                    :TEAM,
                    :TEAMSUB,
                    :STAFFCODE,
                    :STAFFNAME,
                    :DEPARTMENTCODE,
                    :PAIDAMOUNT,
                    :TOTALSALE_MT,
                    :TOTALSALE_NONMT,
                    :TOTALSALE,
                    :LASTESTSALE,
                    :TGD,
                    :TGD_PERCENT,
                    :TOTAL_MT,
                    :TOTAL_NONMT,
                    :TOTAL,
                    :SEQUENCE
                )
                """
                i = 0
                for index, row in df.iterrows():
                    cursor.execute(insert_action_query, {
                        "TEAM": row["TEAM"],
                        "TEAMSUB": row["TEAMSUB"],
                        "STAFFCODE": row["STAFFCODE"],
                        "STAFFNAME": row["STAFFNAME"],
                        "DEPARTMENTCODE": row["DEPARTMENTCODE"],
                        "PAIDAMOUNT": row["PAIDAMOUNT"],
                        "TOTALSALE_MT": row["TOTALSALE_MT"],
                        "TOTALSALE_NONMT": row["TOTALSALE_NONMT"],
                        "TOTALSALE": row["TOTALSALE"],
                        "LASTESTSALE": row["LASTESTSALE"],
                        "TGD": row["TGD"],
                        "TGD_PERCENT": row["TGD_PERCENT"],
                        "TOTAL_MT": row["TOTAL_MT"],
                        "TOTAL_NONMT": row["TOTAL_NONMT"],
                        "TOTAL": row["TOTAL"],
                        "SEQUENCE": row["SEQUENCE"]
                    })
                    print(f"Insert row {i+1}: STAFFCODE={row['STAFFCODE']}, TEAMSUB={row['TEAMSUB']}")
                    i += 1

                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
                conn.commit()
                return df

        except oracledb.Error as error:
            print(error)
            conn.rollback()
            return None

        finally:
            cursor.close()
            conn.close()
                   
    @task
    def insert_CS(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="process_cs_group.select_CS", key="return_value")

        df = result.get("select_CS") if isinstance(result, dict) else None
        cursor, conn = ConOracle_TQMSALE()
        if cursor is None or conn is None:
            return None

        try:
            if df is None or df.empty:
                print("DataFrame is empty. Exit task.")
                return df 
            else:
                insert_action_query = """
                INSERT INTO TQMSALE.TOPSALE(
                    TEAM,
                    TEAMSUB,
                    STAFFCODE,
                    STAFFNAME,
                    DEPARTMENTCODE,
                    PAIDAMOUNT,
                    TOTALSALE_MT,
                    TOTALSALE_NONMT,
                    TOTALSALE,
                    LASTESTSALE,
                    TGD,
                    TGD_PERCENT,
                    TOTAL_MT,
                    TOTAL_NONMT,
                    TOTAL,
                    SEQUENCE
                ) VALUES (
                    :TEAM,
                    :TEAMSUB,
                    :STAFFCODE,
                    :STAFFNAME,
                    :DEPARTMENTCODE,
                    :PAIDAMOUNT,
                    :TOTALSALE_MT,
                    :TOTALSALE_NONMT,
                    :TOTALSALE,
                    :LASTESTSALE,
                    :TGD,
                    :TGD_PERCENT,
                    :TOTAL_MT,
                    :TOTAL_NONMT,
                    :TOTAL,
                    :SEQUENCE
                )
                """
                i = 0
                for index, row in df.iterrows():
                    cursor.execute(insert_action_query, {
                        "TEAM": row["TEAM"],
                        "TEAMSUB": row["TEAMSUB"],
                        "STAFFCODE": row["STAFFCODE"],
                        "STAFFNAME": row["STAFFNAME"],
                        "DEPARTMENTCODE": row["DEPARTMENTCODE"],
                        "PAIDAMOUNT": row["PAIDAMOUNT"],
                        "TOTALSALE_MT": row["TOTALSALE_MT"],
                        "TOTALSALE_NONMT": row["TOTALSALE_NONMT"],
                        "TOTALSALE": row["TOTALSALE"],
                        "LASTESTSALE": row["LASTESTSALE"],
                        "TGD": row["TGD"],
                        "TGD_PERCENT": row["TGD_PERCENT"],
                        "TOTAL_MT": row["TOTAL_MT"],
                        "TOTAL_NONMT": row["TOTAL_NONMT"],
                        "TOTAL": row["TOTAL"],
                        "SEQUENCE": row["SEQUENCE"]
                    })
                    print(f"Insert row {i+1}: STAFFCODE={row['STAFFCODE']}, TEAMSUB={row['TEAMSUB']}")
                    i += 1

                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
                conn.commit()
                return df

        except oracledb.Error as error:
            print(error)
            conn.rollback()
            return None

        finally:
            cursor.close()
            conn.close()
    
    # @task
    # def delete_MK():
    #     cursor, conn = ConOracle_Oracle()
    #     try:
    #         query = "DELETE FROM TQMSALE.TOPSALE@TQMSALE WHERE TEAM = 'MK'"
    #         cursor.execute(query)
    #         conn.commit() 
    #         print("MK data deleted successfully")
    #         return {"delete_MK": "success"}
    #     except oracledb.Error as error:
    #         message = f'เกิดข้อผิดพลาด : {error}'
    #         print(message)
    #         return {"delete_MK": "error"}
    #     finally:
    #         cursor.close()
    #         conn.close()
    
    @task
    def delete_MK():
        cursor, conn = ConOracle_TQMSALE()
        try:
            # 1. ดึงข้อมูลที่จะลบก่อน
            select_query = "SELECT * FROM TQMSALE.TOPSALE WHERE TEAM = 'MK'"
            cursor.execute(select_query)
            records_to_delete = cursor.fetchall()
            
            # 2. แสดงข้อมูลที่จะลบ (แบบ Markdown)
            if records_to_delete:
                df = pd.DataFrame(
                    records_to_delete,
                    columns=[desc[0] for desc in cursor.description]
                )
                print(f"\nกำลังจะลบข้อมูลทีม MK ทั้งหมด {len(records_to_delete)} รายการ:")
                # print(df.to_markdown(index=False))
                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
            else:
                print("ไม่พบข้อมูลทีม MK ในระบบ")
                return {"delete_MK": "success", "deleted_count": 0}
                
            # 3. ลบข้อมูล
            delete_query = "DELETE FROM TQMSALE.TOPSALE WHERE TEAM = 'MK'"
            cursor.execute(delete_query)
            # conn.commit()
            
            print("\nMK data deleted successfully")
            print(f"Total records deleted: {len(records_to_delete)}")
            
            return {
                "delete_MK": "success",
                "deleted_count": len(records_to_delete)
            }
        except oracledb.Error as error:
            conn.rollback()  # Rollback หากเกิดข้อผิดพลาด
            message = f'เกิดข้อผิดพลาดในการลบข้อมูลทีม MK: {error}'
            print(message)
            return {"delete_MK": "error", "message": str(error)}
        finally:
            cursor.close()
            conn.close()

    # @task
    # def delete_MB():
    #     cursor, conn = ConOracle_Oracle()
    #     try:
    #         query = "DELETE FROM TQMSALE.TOPSALE@TQMSALE WHERE TEAM = 'MB'"
    #         cursor.execute(query)
    #         conn.commit() 
    #         print("MB data deleted successfully")
    #         return {"delete_MB": "success"}
    #     except oracledb.Error as error:
    #         message = f'เกิดข้อผิดพลาด : {error}'
    #         print(message)
    #         return {"delete_MB": "error"}
    #     finally:
    #         cursor.close()
    #         conn.close()
    
    @task
    def delete_MB():
        cursor, conn = ConOracle_TQMSALE()
        try:
            # 1. ดึงข้อมูลที่จะลบก่อน
            select_query = "SELECT * FROM TQMSALE.TOPSALE WHERE TEAM = 'MB'"
            cursor.execute(select_query)
            records_to_delete = cursor.fetchall()  # ดึงข้อมูลทั้งหมด
            
            # 2. แสดงข้อมูลที่จะลบ
            print(f"กำลังจะลบข้อมูลทีม MB ทั้งหมด {len(records_to_delete)} รายการ:")
            
            # สร้าง DataFrame เพื่อแสดงผลแบบ Markdown
            if records_to_delete:
                df = pd.DataFrame(
                    records_to_delete,
                    columns=[desc[0] for desc in cursor.description]
                )
                # print(df.to_markdown(index=False))
                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
            else:
                print("ไม่พบข้อมูลทีม MB ในระบบ")
                
            # 3. ลบข้อมูล
            delete_query = "DELETE FROM TQMSALE.TOPSALE WHERE TEAM = 'MB'"
            cursor.execute(delete_query)
            # conn.commit()
            
            print("\nMB data deleted successfully")
            print(f"Total records deleted: {len(records_to_delete)}")
            
            return {
                "delete_MB": "success",
                "deleted_count": len(records_to_delete)
            }
        except oracledb.Error as error:
            conn.rollback()
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return {"delete_MB": "error", "message": message}
        finally:
            cursor.close()
            conn.close()

    # @task
    # def delete_CS():
    #     cursor, conn = ConOracle_Oracle()
    #     try:
    #         query = "DELETE FROM TQMSALE.TOPSALE@TQMSALE WHERE TEAM = 'CS'"
    #         cursor.execute(query)
    #         conn.commit()
    #         print("CS data deleted successfully")
    #         return {"delete_CS": "success"}
    #     except oracledb.Error as error:
    #         message = f'เกิดข้อผิดพลาด : {error}'
    #         print(message)
    #         return {"delete_CS": "error"}
    #     finally:
    #         cursor.close()
    #         conn.close()
    
    @task
    def delete_CS():
        cursor, conn = ConOracle_TQMSALE()
        try:
            # 1. ดึงข้อมูลที่จะลบก่อน
            select_query = "SELECT * FROM TQMSALE.TOPSALE WHERE TEAM = 'CS'"
            cursor.execute(select_query)
            records_to_delete = cursor.fetchall()  # ดึงข้อมูลทั้งหมด
            
            # 2. แสดงข้อมูลที่จะลบ
            print(f"กำลังจะลบข้อมูลทีม CS ทั้งหมด {len(records_to_delete)} รายการ:")
            
            # สร้าง DataFrame เพื่อแสดงผลแบบ Markdown
            if records_to_delete:
                df = pd.DataFrame(
                    records_to_delete,
                    columns=[desc[0] for desc in cursor.description]
                )
                # print(df.to_markdown(index=False))
                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
            else:
                print("ไม่พบข้อมูลทีม CS ในระบบ")
                
            # 3. ลบข้อมูล
            delete_query = "DELETE FROM TQMSALE.TOPSALE WHERE TEAM = 'CS'"
            cursor.execute(delete_query)
            # conn.commit()
            
            print("\nCS data deleted successfully")
            print(f"Total records deleted: {len(records_to_delete)}")
            
            return {
                "delete_CS": "success",
                "deleted_count": len(records_to_delete)
            }
        except oracledb.Error as error:
            conn.rollback()
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return {"delete_CS": "error", "message": message}
        finally:
            cursor.close()
            conn.close()

    start = EmptyOperator(task_id="start_dag", trigger_rule="none_failed_min_one_success")
    end = EmptyOperator(task_id="end_dag", trigger_rule="none_failed_min_one_success")
    holiday_path = EmptyOperator(task_id="Holiday_path", trigger_rule="none_failed_min_one_success")
    work_path = EmptyOperator(task_id="Work_path", trigger_rule="none_failed_min_one_success")
    join_mb = EmptyOperator(task_id="join_mb_branch", trigger_rule="none_failed_min_one_success")
    join_mk = EmptyOperator(task_id="join_mk_branch", trigger_rule="none_failed_min_one_success")
    join_cs = EmptyOperator(task_id="join_cs_branch", trigger_rule="none_failed_min_one_success")

    # Define task groups
    @task_group(group_id="process_mk_group")
    def process_mk_tasks():
        delete_task = delete_MK()
        select_task = select_MK()
        insert_task = insert_MK()
        delete_task >> select_task >> insert_task
    
    @task_group(group_id="process_mb_group")
    def process_mb_tasks():
        delete_task = delete_MB()
        select_task = select_MB()
        insert_task = insert_MB()
        delete_task >> select_task >> insert_task
    
    @task_group(group_id="process_cs_group")
    def process_cs_tasks():
        delete_task = delete_CS()
        select_task = select_CS()
        insert_task = insert_CS()
        delete_task >> select_task >> insert_task

    # Define workflow
    check_holiday_task = check_holiday()
    
    (
        start >> check_holiday_task >> [holiday_path, work_path],
        holiday_path >> end,
        work_path >> [join_mk, join_mb, join_cs],
        join_mk >> process_mk_tasks() >> end,
        join_mb >> process_mb_tasks() >> end,
        join_cs >> process_cs_tasks() >> end
    )