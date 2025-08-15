import configparser
from airflow import DAG
from airflow.decorators import task, dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
import oracledb
import json
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

cmtel_config_str = config.get("variable", "CMTEL_config")
cmt21_config_str = config.get("variable", "CMT21_config")

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

def Get_Actions():
    cursor, conn = ConOracle()
    try:
        cursor.execute(
            """
                SELECT * FROM XININSURE."ACTION" a
                ORDER BY a.ACTIONID
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
        print(f"Get_Actions : {e}")
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
    
def Check_action_code(row, df_actionData):
    def check_CMT21(staffCode, cmt21_config):
        team_mapping = {
            'team_g': 'CMT 16 G', 'team_d': 'CMT 16 D', 'team_m': 'CMT 16 M',
            'team_e': 'CMT 16 E', 'team_k': 'CMT 16 K', 'team_b': 'CMT 16 B',
            'team_f': 'CMT 16 F', 'team_j': 'CMT 16 J', 'team_c': 'CMT 16 C',
            'team_i': 'CMT 16 I', 'team_n': 'CMT 16 N', 'team_h': 'CMT 16 H'
        }

        for team, code in team_mapping.items():
            if staffCode in cmt21_config.get(team, []):
                return code
        return 'MK02' # else case if no team matches
    
    def check_CMTEL(staffCode, cmtel_config):
        code = ''
        if staffCode in cmtel_config.get("team_a", []):
            code = 'ESY01A'
        elif staffCode in cmtel_config.get("team_b", []):
            code = 'ESY01B'
        return code
    
    try:
        cmtel_config = json.loads(cmtel_config_str)
        cmt21_config = json.loads(cmt21_config_str)

        actionCode = row["ACTIONCODE"]
        staffCode = row["STAFFCODE"]
        code = ""

        if actionCode == "CMTEL":
            code = check_CMTEL(staffCode, cmtel_config)
        elif actionCode == "CMT21":
            code = check_CMT21(staffCode, cmt21_config)
        elif actionCode in ["CMT34","CMT34.01","CMT35", "CMT35.01"]:
            code = 'ZDL01'
        elif actionCode in ["CMT32", "CMT32.01", "CMT33"]:
            region = row["REGION"]
            if region in ['N']:
                code = 'ZBR01'
            elif region in ['BKK', 'MX', 'MU', 'ML']:
                code = 'ZBR02'
            elif region in ['NL', 'NU']:
                code = 'ZBR03'
            elif region in ['S']:
                code = 'ZBR04'
        
        print(code)
        if code:
            result = df_actionData.query("ACTIONCODE == @code")["ACTIONID"]
            print(f"Actioncode: {code}, Result: {result} ")
            if not result.empty:
                print(f"result:!! {result.iloc[0]}")
                return result.iloc[0]
        return None

    except Exception as e:
        print(f"Check action code error: {e}")
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
                        s.salebookcode,
                        s.routeid ,
                        B.region,
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
            F.STAFFCODE,
            F.STAFFTYPE,
            F.STAFFCODE || ':' || F.STAFFNAME AS STAFFNAME,
            D.DEPARTMENTID,
            D.DEPARTMENTCODE || ':' || D.DEPARTMENTNAME AS DEPARTMENTNAME,
            D.DEPARTMENTCODE,
            D.DEPARTMENTGROUP,
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
                PT.PRODUCTGROUP,
                PT.PRODUCTTYPE
            FROM
                XININSURE.SALE S,
                XININSURE.STAFF F,
                XININSURE.DEPARTMENT D,
                XININSURE.PRODUCT P,
                XININSURE.PRODUCTTYPE PT,
                XININSURE.SUPPLIER SU,
                XININSURE.ROUTE R,
                XININSURE.BRANCH B,
                (
                SELECT
                    SA.SALEID ,
                    SA.INSTALLMENT,
                    a.actionid,
                    a.actioncode ,
                    sa.actionstatus,
                    sa.RESULTID,
                    sa.duedate,
                    sa.sequence
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
                        AND SA.DUEDATE = TO_DATE('30/07/2024', 'DD/MM/YYYY')
            --    AND SA.DUEDATE = TRUNC(SYSDATE) 
            --    AND SA.DUEDATE >= TO_DATE('25/07/2024', 'DD/MM/YYYY')
            --   AND SA.DUEDATE <= TO_DATE('30/07/2024', 'DD/MM/YYYY')
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
                AND B.BRANCHID = R.BRANCHID
            --    AND PT.PRODUCTGROUP = 'MT'
                AND SU.SUPPLIERCODE IN ('KWIL','AIA','SSL','BKI','BLA','VY','DHP','TVI','MTI','MTL','MLI','ALIFE','FWD','KTAL','ACE','SELIC','PLA','TSLI', 'ESY')
            --    AND SU.SUPPLIERCODE IN ('ESY')
                --อนุมัติ
                AND S.PLATEID IS NULL
                AND S.CANCELDATE IS NULL
                AND S.CANCELEFFECTIVEDATE IS NULL
            --    AND S.SALEID = 43415566
            --    AND PT.PRODUCTTYPE IN ('LOAN', 'LOANX')
            --    FETCH FIRST 1 ROWS only
            ORDER BY
                s.SALEID DESC """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )
            
            df_digital_room = df.query("DEPARTMENTGROUP in ('DM')")
            
            # df_esy02 = df.query("ACTIONCODE in ('ESY02') and ACTIONSTATUS in ('Y')")
            
            formatted_table = df.to_markdown(index=False)
            # print(query)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")
            
            return { 'df_cancel_work': df, 'df_digital_room': df_digital_room }
        
        except oracledb.Error as e:
            print(f"Get_Data : {e}")
            
        finally:
            cursor.close()
            conn.close()          
                
    @task
    def Insert_digital_DM(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="Get_cancelled_work", key="return_value")

        df = result.get("df_digital_room", pd.DataFrame())
        
        cursor, conn = ConOracle()
        if cursor is None or conn is None:
            return None
        
        try:
            if df.empty:
                print("DataFrame is empty. Exiting task.")
                return df
            else:

                i = 0
                for index, row in df.iterrows():
                    action_code_insert = None
                    request_remark = "Auto Cancel MT ห้องขาย DM กรุณาตรวจสอบเพิ่มเติมก่อนยกเลิก"
                    action_status_update = "W"

                    cursor.execute("""SELECT ACTIONID FROM XININSURE.ACTION a WHERE a.ACTIONCODE = 'CMT105.1'""")
                    action_code_insert = cursor.fetchone()[0]

                    if action_code_insert:
                        insert_action_query = """
                        INSERT INTO XININSURE.SALEACTION(SALEID, SEQUENCE, ACTIONID, ACTIONSTATUS, DUEDATE, REQUESTREMARK, ACTIONREMARK)
                        SELECT X.SALEID,
                            NVL((SELECT MAX(SEQUENCE) FROM XININSURE.SALEACTION WHERE SALEID = :saleid), 0) + 1,
                            :actionid,
                            :actionstatus,
                            TRUNC(SYSDATE),
                            :request_remark,
                            '' 
                        FROM XININSURE.SALE X
                        WHERE X.SALEID = :saleid
                        """
                        cursor.execute(insert_action_query, {
                            "saleid": row["SALEID"],
                            "actionid": action_code_insert,
                            "actionstatus": action_status_update,
                            "request_remark": request_remark,
                        })
                        print(f"Insert {i+1}: SALEID={row['SALEID']}, ACTIONID={action_code_insert}")
                        i += 1

                conn.commit() 
                return df

        except oracledb.Error as error:
            print(f"OracleDB Error: {error}")
            conn.rollback()
            return None

        finally:
            cursor.close()
            conn.close() 
        
    @task
    def Select_esy02_X(**kwargs):
        cursor, conn = ConOracle()
        
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="Get_cancelled_work", key="return_value")
        
        df = result.get("df_cancel_work", pd.DataFrame())
        
        if cursor is None or conn is None:
            return pd.DataFrame()  # Return empty DataFrame
        
        try:
            if df.empty:
                print("DataFrame is empty.")
                return pd.DataFrame()  # Return empty DataFrame
            
            # Query สำหรับเช็คว่ามี ESY02 หรือไม่ (COUNT เท่านั้น)
            check_esy02_query = """
                SELECT COUNT(*) as record_count
                FROM XININSURE.SALE S
                INNER JOIN XININSURE.SALEACTION sa ON S.SALEID = sa.SALEID
                INNER JOIN XININSURE.ACTION a ON sa.ACTIONID = a.ACTIONID
                WHERE S.SALEID = :SALEID
                AND sa.ACTIONSTATUS = 'Y'
                AND a.ACTIONCODE = 'ESY02'
                AND sa.sequence = (
                    SELECT MAX(sa_max.sequence) FROM XININSURE.SALEACTION sa_max
                    INNER JOIN XININSURE.ACTION a_max ON sa_max.ACTIONID = a_max.ACTIONID
                    WHERE sa_max.SALEID = sa.SALEID AND sa_max.ACTIONSTATUS = 'Y' 
                    AND a_max.ACTIONCODE = 'ESY02'
                )
                AND S.PLATEID IS NULL
                AND S.CANCELDATE IS NULL
                AND S.CANCELEFFECTIVEDATE IS NULL
            """
            
            # เก็บ index ของ row ที่ผ่านเงื่อนไข
            valid_indices = []
            
            i = 0
            for index, row in df.iterrows():
                saleid = row['SALEID']
                
                # เช็คว่ามี ESY02 หรือไม่
                cursor.execute(check_esy02_query, {'SALEID': saleid})
                count_result = cursor.fetchone()
                record_count = count_result[0] if count_result else 0
                
                if record_count >= 1:
                    valid_indices.append(index)
                    print(f"Number: {i+1} Found ESY02 for SALEID={saleid} - Added to result")
                else:
                    print(f"Number: {i+1} No ESY02 found for SALEID={saleid} - Skipped")
                i += 1
            
            # กรอง DataFrame ตาม valid_indices
            df_result = df.loc[valid_indices].copy() if valid_indices else pd.DataFrame()
            
            formatted_table = df_result.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Select ESY02 data successfully")
            print(f"Total records found: {len(df_result)}")
            print(f"Total SALEIDs checked: {len(df)}")
            request_remark= "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้วไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ"
            action_status="X"
            
            conn.commit()
            
            return { 'action_status' : action_status , 'request_remark' : request_remark, 'Select_esy02_X' : df_result }
            
        except oracledb.Error as error:
            conn.rollback()  
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return pd.DataFrame()  # Return empty DataFrame on error
        finally:
            cursor.close()
            conn.close()
            

    @task #ตั้ง default ไปก่อนกัน error แก้ทีหลัง
    def Set_action_code(action_status = "X", request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้วไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ", **kwargs, ):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="Select_esy02_X", key="return_value")

        df = result.get("Select_esy02_X", pd.DataFrame())
        cursor, conn = ConOracle()
        
        df_actionData = Get_Actions()
        
        if df_actionData is None or df_actionData.empty:
            print("Failed to get action data.")
            cursor.close()
            conn.close()
            return None

        sql = """
                INSERT INTO XININSURE.SALEACTION(SALEID, SEQUENCE, ACTIONID, ACTIONSTATUS, DUEDATE, REQUESTREMARK)
                SELECT S.SALEID,
                    NVL((SELECT MAX(SEQUENCE) FROM XININSURE.SALEACTION WHERE SALEID = :saleid), 0) + 1,
                    :actionid,
                    :actionstatus,
                    TRUNC(SYSDATE),
                    :request_remark
                FROM XININSURE.SALE S
                WHERE S.SALEID = :saleid
            """
        # action_status = "X"
        # request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้วไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ"
        try:

            for index, row in df.iterrows():
                print("Fetching data...")
                ResActionCode = Check_action_code(row, df_actionData) ## df.row
                if ResActionCode is not None:
                    cursor.execute(sql, {
                        "saleid": row["SALEID"],
                        "actionid": int(ResActionCode),
                        "actionstatus" : action_status,
                        "request_remark" : request_remark
                    })
                    print(f"Insert {index}: SALEID={row['SALEID']}, ACTIONID={ResActionCode}")

            formatted_table = df.to_markdown(index=False)
            # print(query)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df)}")

            conn.commit() 
            return { 'Set_action_code': df }
        
        except oracledb.Error as e:
            print(f"Get_Data : {e}")
        finally:
            cursor.close()
            conn.close()
            
    # ตั้งโค๊ด C21 กรณีชำระหลังมีการตั้ง Code ยกเลิกไปแล้ว และ เป็นศูนย์ประสานงาน
    # def Insert_C21_Routes_deadline(**kwargs):
    #         ti = kwargs["ti"]
    #         result = ti.xcom_pull(task_ids="Process_x.Split_actioncode_dl", key="return_value")

    #         df = result.get("df_Routes", pd.DataFrame())

    #         cursor, conn = ConOracle()
    #         if cursor is None or conn is None:
    #             return None

            

    #         try:
    #             if df.empty:
    #                 print("DataFrame is empty. Exiting task.")
    #                 return df
    #             else:
    #                 # Define region codes
    #                 region_codes = {
    #                     "ภาคเหนือ": [
    #                         "005", "017", "023", "026", "033", "034", "035", "052", "053",
    #                         "065", "075", "013", "014", "036", "038", "044"
    #                     ],
    #                     "ภาคใต้": [
    #                         "002", "012", "016", "022", "025", "031", "039", "040", "041",
    #                         "045", "048", "057", "058", "067"
    #                     ],
    #                     "ภาคตะวันออกเฉียงเหนือ": [
    #                         "004", "006", "011", "020", "021", "079", "027", "043", "042",
    #                         "046", "047", "055", "056", "068", "069", "070", "076", "074",
    #                         "072", "054"
    #                     ],
    #                     "ภาคกลาง": [
    #                         "001", "024", "028", "003", "007", "008", "009", "010", "015",
    #                         "018", "019", "029", "030", "032", "049", "050", "051", "060",
    #                         "059", "061", "063", "062", "064", "066", "073", "071", "037"
    #                     ]
    #                 }

    #                 i = 0
    #                 for index, row in df.iterrows():
    #                     action_code_insert = None
    #                     request_remark = "ลูกค้าชําระเข้ามาหลังตั้งโค้ดยกเลิกรบกวนสรุปงานหากต้องการยกเลิกให้ตั้งโค้ดยกเลิกอีกครั้ง"
    #                     action_status_update = "W"

    #                     if row["PROVINCECODE"] in region_codes["ภาคเหนือ"]:
    #                         cursor.execute("""SELECT ACTIONID FROM XININSURE.ACTION WHERE ACTIONCODE = 'ZBR01'""")
    #                         action_code_insert = cursor.fetchone()[0]

    #                     elif row["PROVINCECODE"] in region_codes["ภาคใต้"]:
    #                         cursor.execute("""SELECT ACTIONID FROM XININSURE.ACTION WHERE ACTIONCODE = 'ZBR02'""")
    #                         action_code_insert = cursor.fetchone()[0]

    #                     elif row["PROVINCECODE"] in region_codes["ภาคตะวันออกเฉียงเหนือ"]:
    #                         cursor.execute("""SELECT ACTIONID FROM XININSURE.ACTION WHERE ACTIONCODE = 'ZBR03'""")
    #                         action_code_insert = cursor.fetchone()[0]

    #                     elif row["PROVINCECODE"] in region_codes["ภาคกลาง"]:
    #                         cursor.execute("""SELECT ACTIONID FROM XININSURE.ACTION WHERE ACTIONCODE = 'ZBR04'""")
    #                         action_code_insert = cursor.fetchone()[0]

    #                     if action_code_insert:
    #                         insert_action_query = """
    #                         INSERT INTO XININSURE.SALEACTION(SALEID, SEQUENCE, ACTIONID, ACTIONSTATUS, DUEDATE, REQUESTREMARK, ACTIONREMARK)
    #                         SELECT X.SALEID,
    #                             NVL((SELECT MAX(SEQUENCE) FROM XININSURE.SALEACTION WHERE SALEID = :saleid), 0) + 1,
    #                             :actionid,
    #                             :actionstatus,
    #                             TRUNC(SYSDATE) + 1,
    #                             :request_remark,  -- Static value for requestremark
    #                             ''  -- Use the dynamically set actionremark
    #                         FROM XININSURE.SALE X
    #                         WHERE X.SALEID = :saleid
    #                         """
    #                         cursor.execute(insert_action_query, {
    #                             "saleid": row["SALEID"],
    #                             "actionid": action_code_insert,
    #                             "actionstatus": action_status_update,
    #                             "request_remark": request_remark,
    #                         })
    #                         print(f"Insert {i+1}: SALEID={row['SALEID']}, ACTIONID={action_code_insert}")
    #                         i += 1

    #                 #conn.commit() 
    #                 return df

    #         except oracledb.Error as error:
    #             print(f"OracleDB Error: {error}")
    #             conn.rollback()
    #             return None

    #         finally:
    #             cursor.close()
    #             conn.close()
                
                
    # @task
    # def insert_task_deadline(**kwargs):

    #     task_id = kwargs['task_instance'].task_id
    #     try_number = kwargs['task_instance'].try_number
    #     message = f"Processing task {task_id} ,try_number {try_number}"
    #     print(f"{message}")
    #     try:
    #         # เรียกใช้ฟังก์ชัน Insert_M13_deadline
    #         print("Running Insert_M13_deadline...")
    #         df_C21 = Insert_C21_Routes_deadline(**kwargs)
    #         if df_C21 is not None:
    #             print(f"Insert_C21_Routes_deadline {len(df_C21)} records.")


    #         print("All tasks completed successfully.")
    #         # print (f'{"SUM insert deadline",(len(df_C21))}')
    #         return True

    #     except Exception as e:
    #         message = f"Fail with task {task_id} \n error : {e}"
    #         print(f"Error in insert_task_deadline: {e}")
    #         return None

    #Dummy
    start = EmptyOperator(task_id="start_dag", trigger_rule="none_failed_min_one_success")
    end = EmptyOperator(task_id="end_dag", trigger_rule="none_failed_min_one_success")
    holiday_path = EmptyOperator(task_id="Holiday_path", trigger_rule="none_failed_min_one_success")
    work_path = EmptyOperator(task_id="Work_path", trigger_rule="none_failed_min_one_success")
    join_x = EmptyOperator(task_id="join_x_branch", trigger_rule="none_failed_min_one_success")
    join_v = EmptyOperator(task_id="join_v_branch", trigger_rule="none_failed_min_one_success")
    # execute_x = EmptyOperator(task_id="execute_x_path", trigger_rule="none_failed_min_one_success")
    execute_v = EmptyOperator(task_id="execute_v_path", trigger_rule="none_failed_min_one_success")
    # get_cancellation_work = EmptyOperator(task_id="get_cancellation_work", trigger_rule="none_failed_min_one_success")
    
    #task ที่ไป call function
    check_holiday_task = check_holiday()
    get_cancellation_work = Get_cancelled_work()
    execute_x = Set_action_code()
    get_esy02_X_task = Select_esy02_X()
    insert_digital_DM_task = Insert_digital_DM()
    
    #กำหนด workflow
    (
        #เริ่มต้น
        start >> check_holiday_task >> [holiday_path, work_path],
        holiday_path >> end,
        
        #ระงับยกเลิกไป join x , ยกเลิกไป join v
        work_path >> get_cancellation_work >> get_esy02_X_task >> insert_digital_DM_task >> [join_x, join_v],
        
        # แทนค่าด้วยฟังก์ชันระงับยกเลิกได้เลย
        join_x >> execute_x >> end,
        
        join_v >> execute_v >> end,
    )