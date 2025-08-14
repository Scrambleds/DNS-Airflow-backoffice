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

config_file_path = 'config/Smart_cancel.cfg'
config = configparser.ConfigParser()
config.read(config_file_path)
local = config.get("variable","tz")
local_tz = pendulum.timezone(local)
currentDateAndTime = datetime.now(tz=local_tz)
currentDate = currentDateAndTime.strftime("%Y-%m-%d")

def ConOracle():
    try:
        env = os.getenv('ENV', 'xininsure_dev')
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
    dag_id="Smart_cancel_mt",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    description="Smart cancel motor",
    tags=["DCP"],
    start_date=datetime(2024, 4, 24, 16, 30, 0, 0, tzinfo=local_tz),
    # start_date=start_date,  # ใช้ start_date ที่คำนวณแล้ว
    schedule_interval="*/50 8-20 * * *",
    # schedule_interval="*/10 8-20 * * *"
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
                return "Work_path"
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"check_holiday : {e}")
        finally:
            print(f"{message}")
            
    @task
    def Get_cancelled_work():
        cursor, conn = ConOracle()
        try:
            query = f"""SELECT
                        S.SALEID,
                        xininsure.getbookname(s.periodid,
                        s.salebookcode,
                        s.sequence) AS bookname,
                        s.routeid ,
                        r.routecode,
                        s.paidamount,
                        s.cancelresultid,
                        (
                        SELECT
                            SB.BYTECODE
                        FROM
                            XININSURE.SYSBYTEDES SB
                        WHERE
                            SB.COLUMNNAME = 'PAYMENTSTATUS'
            AND SB.TABLENAME = 'SALE'
                AND SB.BYTECODE = S.PAYMENTSTATUS) AS PAYMENTSTATUS,
            S.PAYMENTMODE,
            F.STAFFCODE || ':' || F.STAFFNAME AS STAFFNAME,
            D.DEPARTMENTID,
            D.DEPARTMENTCODE || ':' || D.DEPARTMENTNAME AS DEPARTMENTNAME,
            D.DEPARTMENTCODE,
            ssa.actionid,
            ssa.actioncode,
            ssa.actionstatus,
            (
            SELECT
                r.PROVINCECODE
            FROM
                XININSURE.ROUTE r
            WHERE
                s.ROUTEID = r.ROUTEID) AS PROVINCECODE,
            ssa.RESULTID,
            (
            SELECT
                r.RESULTCODE
            FROM
                XININSURE.RESULT r
            WHERE
                r.RESULTID = ssa.RESULTID) AS RESULTCODE,
            S.MASTERSALEID,
            ssa.duedate,
            (SELECT MAX(r.SAVEDATE ) 
                FROM XININSURE.RECEIVEITEMCLEAR T , 
                        xininsure.receiveitem i,
                        xininsure.receive    r              
                WHERE  t.receiveid = i.receiveid
                and    t.receiveitem = i.receiveitem
                and    i.receiveid = r.receiveid
                and    r.receivestatus in ('S','C')
            and    i.receivebookcode not  in('R01','R03','R02','R04')
                and   T.SALEID = SSA.SALEID   )  AS LASTUPDATEDATETIME,
            (
            SELECT
                max(rc.installment)
                -- ชำระล่าสุดงวดที่
            FROM
                xininsure.receiveitem ri,
                xininsure.receiveitemclear rc
            WHERE
                ri.receiveid = rc.receiveid
                AND rc.receiveitem = rc.receiveitem
                AND ri.saleid = S.SALEID) AS LASTINSTALLMENT,
            (
            SELECT
                max(sa.installment)
                ---  งานที่ตามอยู่
            FROM
                xininsure.saleaction sa
            WHERE
                sa.actionid = ssa.actionid
                AND sa.actionstatus = 'W'
            AND sa.saleid = S.SALEID
            AND sa.sequence = (
            SELECT
                max(sa.sequence)
            FROM
                xininsure.saleaction sa
            WHERE
                sa.actionid = ssa.actionid
                AND sa.actionstatus = 'W'
                        AND sa.saleid = S.SALEID)) AS FOLLOWINSTALLMENT,
                        su.SUPPLIERCODE,
            NVL(
                    (
                    SELECT
                        s.BALANCE
                    FROM
                        xininsure.salepayment s
                    WHERE
                        s.saleid = ssa.saleid
                        AND s.balance > 0
                        AND s.installment = (
                            SELECT
                                sa.installment
                            FROM
                                xininsure.saleaction sa
                            WHERE
                                sa.saleid = ssa.saleid
                                AND sa.actionid = 44
                                -- C01
                AND sa.actionstatus = 'W'
                AND sa.sequence = (
                    SELECT
                        max(sa.sequence)
                    FROM
                        xininsure.saleaction sa
                    WHERE
                        sa.saleid = ssa.saleid
                        AND sa.actionid = 44
                        -- C01
                        AND sa.actionstatus = 'W'
                                )
                        )
                    ),
                    0
                )AS balance,
                PT.PRODUCTGROUP
            FROM
                XININSURE.SALE S,
                XININSURE.STAFF F,
                XININSURE.DEPARTMENT D,
                XININSURE.PRODUCT P,
                XININSURE.PRODUCTTYPE PT,
                XININSURE.SUPPLIER SU,
                XININSURE.ROUTE R,
                (
                SELECT
                    SA.SALEID ,
                    SA.INSTALLMENT,
                    a.actionid,
                    a.actioncode ,
                    sa.actionstatus,
                    sa.RESULTID,
                    sa.duedate
                    FROM
                    XININSURE.SALEACTION SA,
                    xininsure.action a
                WHERE
                    SA.ACTIONID IN(3760, 3761, 2261, 3740, 3741, 3742, 3743,
                    5933, 11293, 7533, 9133, 9174, 11574, 11575, 11576, 
                    11577, 11553, 11554, 11555, 15014)
                        AND sa.actionid = a.actionid
                        --ดำเนินการแล้ว
                        AND SA.ACTIONSTATUS IN ('R', 'W', 'Y')
            --    AND SA.DUEDATE = TRUNC(SYSDATE) 
                AND SA.DUEDATE >= TO_DATE('20/07/2024', 'DD/MM/YYYY')
                AND SA.DUEDATE <= TO_DATE('30/07/2024', 'DD/MM/YYYY')
                --AND    SA.DUEDATE <= TRUNC(SYSDATE) - 1
                --AND sa.duedate >= to_date('07/05/2025', 'dd/mm/yyyy')
                --AND sa.duedate <= to_date('10/05/2020', 'dd/mm/yyyy') 
                                ) SSA
            WHERE
                S.SALEID = SSA.SALEID
                AND S.STAFFID = F.STAFFID
                AND S.STAFFDEPARTMENTID = D.DEPARTMENTID
                AND S.ROUTEID = R.ROUTEID
                AND S.PRODUCTID = P.PRODUCTID
                AND P.SUPPLIERID = SU.SUPPLIERID
                AND P.PRODUCTTYPE = PT.PRODUCTTYPE
            --    AND PT.PRODUCTGROUP = 'MT'
                AND SU.SUPPLIERCODE IN ('KWIL','AIA','SSL','BKI','BLA','VY','DHP','TVI','MTI','MTL','MLI','ALIFE','FWD','KTAL','ACE','SELIC','PLA','TSLI')
                --อนุมัติ
                AND S.PLATEID IS NULL
                AND S.CANCELDATE IS NULL
                AND S.CANCELEFFECTIVEDATE IS NULL
            ORDER BY
                s.SALEID DESC """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            # print(query)
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            return { 'df_cancel_work': df }
        
        except oracledb.Error as e:
            print(f"Get_Data : {e}")
            
        finally:
            cursor.close()
            conn.close()           

    #Dummy
    start = EmptyOperator(task_id="start_dag", trigger_rule="none_failed_min_one_success")
    end = EmptyOperator(task_id="end_dag", trigger_rule="none_failed_min_one_success")
    holiday_path = EmptyOperator(task_id="Holiday_path", trigger_rule="none_failed_min_one_success")
    work_path = EmptyOperator(task_id="Work_path", trigger_rule="none_failed_min_one_success")
    join_x = EmptyOperator(task_id="join_x_branch", trigger_rule="none_failed_min_one_success")
    join_v = EmptyOperator(task_id="join_v_branch", trigger_rule="none_failed_min_one_success")
    execute_x = EmptyOperator(task_id="execute_x_path", trigger_rule="none_failed_min_one_success")
    execute_v = EmptyOperator(task_id="execute_v_path", trigger_rule="none_failed_min_one_success")
    # get_cancellation_work = EmptyOperator(task_id="get_cancellation_work", trigger_rule="none_failed_min_one_success")
    
    #task ที่ไป call function
    check_holiday_task = check_holiday()
    get_cancellation_work = Get_cancelled_work()
    
    #กำหนด workflow
    (
        #เริ่มต้น
        start >> check_holiday_task >> [holiday_path, work_path],
        holiday_path >> end,
        
        #ระงับยกเลิกไป join x , ยกเลิกไป join v
        work_path >> get_cancellation_work >> [join_x, join_v],
        
        # แทนค่าด้วยฟังก์ชันระงับยกเลิกได้เลย
        join_x >> execute_x >> end,
        
        join_v >> execute_v >> end,
    )