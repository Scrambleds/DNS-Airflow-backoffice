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
from linebot import LineBotApi
from linebot.exceptions import LineBotApiError
from linebot.models import TextSendMessage
from linebot.models import FlexSendMessage
import asyncio
# import cx_Oracle

config_file_path = 'config/Smart_cancel.cfg'
config = configparser.ConfigParser()
config.read(config_file_path)
local = config.get("variable","tz")
local_tz = pendulum.timezone(local)
currentDateAndTime = datetime.now(tz=local_tz)
currentDate = currentDateAndTime.strftime("%Y-%m-%d")
currentTime = currentDateAndTime.strftime("%H:%M:%S")
cmtel_config_str = config.get("variable", "CMTEL_config")
cmt21_config_str = config.get("variable", "CMT21_config")
invalid_action_codes_str = config.get("variable", "invalid_action_codes")
cancel_messages_str = config.get("variable", "cancel_messages")

invalid_action_codes = json.loads(invalid_action_codes_str)
line_access_token = config.get('variable', 'channel_access_token')
line_user_id = config.get('variable', 'line_user_id')
line_bot_api = LineBotApi(config.get('variable', 'channel_access_token'))

def send_flex_notification_start(message=None):
    """
    ส่ง Flex Message เริ่มต้นทำงาน
    :param message: ข้อความเพิ่มเติมในกรณี error (optional)
    """
    try:
        contents = {
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "text",
                        "text": "AUTOMATION",
                        "color": "#00d610",
                        "size": "sm",
                        "weight": "bold",
                        "margin": "xs"
                    },
                    {
                        "type": "text",
                        "text": "Smart cancel MT (Phase 2)",
                        "size": "lg",
                        "margin": "xl",
                        "color": "#727272",
                        "weight": "bold"
                    },
                    {
                        "type": "text",
                        "text": "[ระบบเริ่มทำงาน]" if not message else "[เกิดข้อผิดพลาด]",
                        "margin": "none",
                        "size": "lg",
                        "weight": "bold",
                        "color": "#727272"
                    },
                    {
                        "type": "text",
                        "text": f"วันที่ {currentDate} เวลา {currentTime} น.",
                        "margin": "lg",
                        "size": "xs",
                        "color": "#a6a6a6"
                    },
                    {
                        "type": "separator",
                        "margin": "lg"
                    }
                ]
            }
        }
        
        if message:
            contents["body"]["contents"].append({
                "type": "text",
                "text": "รายละเอียดข้อผิดพลาด:",
                "margin": "lg",
                "size": "md",
                "weight": "bold",
                "color": "#000000"
            })
            contents["body"]["contents"].append({
                "type": "text",
                "text": message,
                "margin": "md",
                "size": "sm",
                "color": "#000000",
                "wrap": True
            })
        
        # สร้าง Flex Message
        flex_message = FlexSendMessage(
            alt_text="สถานะการทำงาน DNC",
            contents=contents
        )

        # ส่งข้อความ
        # line_bot_api = LineBotApi(line_access_token)
        line_bot_api.push_message(line_user_id, flex_message)
        
        logging.info("Flex notification sent successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error sending Flex notification: {e}")
        return False
    

def ConOracle():
    try:
        env = os.getenv('ENV', 'xininsure_preprod_demo')
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
        # send_flex_notification_start(message)
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
        elif actionCode in invalid_action_codes:
            code = 'CMT105.1'
        
        print("action code :",actionCode, "change to : ",code)
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
    
def check_cancel_CMT21(row):
    sql = """
        SELECT a.ACTIONCODE FROM XININSURE.SALEACTION sa
        JOIN XININSURE."ACTION" a ON sa.ACTIONID = a.ACTIONID
        WHERE sa.SALEID = :saleid
            AND sa.ACTIONSTATUS IN ('Y')
        ORDER BY sa.SEQUENCE
    """
    cursor, conn = ConOracle()
    try:
        cursor.execute(sql, {"saleid": row["SALEID"].iloc[0]})
        result = cursor.fetchall()
        if result:
            action_codes = [row[0] for row in result]
            print(f"Action codes of {row["SALEID"].iloc[0]} is : {action_codes}")
            if not set(['ZCL08','ZCL09','ZCL10','ZCL16']).isdisjoint(action_codes):
                return 'MK'
            elif not set(['CMT25']).isdisjoint(action_codes):
                return 'BR'
            elif not set(['CMT25']).isdisjoint(action_codes):
                return 'DL'
            else:
                return 'CL'
        else:
            raise
    except Exception as e:
        print(f"Error checking CMT21: {e}")
        raise

            
    
def Get_request_cancel_by(row):
    try:
        actionCode = row["ACTIONCODE"]
        cancelBy = ""
        if actionCode == "CMTEL":
            cancelBy = 'ESY'
        elif actionCode == "CMT21":
            cancelBy = check_cancel_CMT21(row)
        elif actionCode in ["CMT21.1"]:
            cancelBy = 'COMP'
        elif actionCode in ["CMT32", "CMT32.01", "CMT33"]:
            cancelBy = 'BR'
        elif actionCode in ["CMT34","CMT34.01","CMT35", "CMT35.01"]:
            cancelBy = 'DL'
        elif actionCode in ["CMT36","CMT36.01","CMT37"]:
            cancelBy = 'CM'
        elif actionCode in ["CMT90"]:
            cancelBy = 'MK'
        elif actionCode in ["CMT91"]:
            cancelBy = 'OP'
        elif actionCode in ["CMT92","CMT92.1","CMT92.2","CMT92.3","CMT92.4"]:
            cancelBy = 'COS'
        return cancelBy
    except Exception as e:
        print(f"Get request cancel by error: {e}")
        raise e

def Set_result_cancel(df = pd.DataFrame(), isBefore3PM = False):
    cursor, conn = ConOracle()
    
    if df is None or df.empty:
        print("Failed to get data.")
        cursor.close()
        conn.close()
        return None
    
    field_cond = ""
    var_cond = {}

    sql = """
            UPDATE
                XININSURE.SALE
            SET
                {0}
            WHERE
                X.SALEID = :saleid
        """
    # action_status = "X"
    # request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้วไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ"
    try:

        for index, row in df.iterrows():
            print("Fetching data...")
            cancel_by = Get_request_cancel_by(row)
            remark = ""
            
            if row["ACTIONREMARK"] is not None:
                remark = row["ACTIONREMARK"]
            if row["ACTIONREMARK"] is None and row["REQUESTREMARK"] is not None:
                remark = row["REQUESTREMARK"]
            if row['RESULTCODE'].iloc[0] in ['XPOL']:
                field_cond = """
                    CANCELDATE = :cancel_date,
                    CACELRESULTID = :resultid,
                    OTHERCANCEL = :remark,
                    CANCELREQUESTBY = :cancel_by
                """
            elif row['RESULTCODE'].iloc[0] in ['XPRB']:
                field_cond = """
                    CANCELDATE = :cancel_date,
                    PRBCANCELRESULTID = :resultid,
                    OTHERCANCELPRB = :remark,
                    CANCELREQUESTBY = :cancel_by
                """
            elif row['RESULTCODE'].iloc[0] in ['XALL']:
                field_cond = """
                    CANCELDATE = :cancel_date,
                    CACELRESULTID = :resultid,
                    OTHERCANCEL = :remark,
                    PRBCANCELDATE = :cancel_date,
                    PRBCANCELRESULTID = :resultid,
                    OTHERCANCELPRB = :remark,
                    CANCELREQUESTBY = :cancel_by
                """
            var_cond = {
                "cancel_date" : "TRUNC(SYSDATE)" if isBefore3PM else "TRUNC(SYSDATE) + INTERVAL '1' DAY",
                "resultid" : row["RESULTID"].iloc[0],
                "remark" : remark,
                "cancel_by" : cancel_by
            }
            cursor.execute(sql.format(field_cond), {
                "saleid": row["SALEID"],
                **var_cond
            })

        formatted_table = df.to_markdown(index=False)
        # print(query)
        print(f"\n{formatted_table}")
        print(f"Get data successfully")
        print(f"df: {len(df)}")

        #conn.commit()  
        return { 'Set_action_code': df }
    
    except Exception as e:
        print(f"Exception : {e}")
    finally:
        cursor.close()
        conn.close()

def Set_sale_action_status(df = pd.DataFrame()):
    cursor, conn = ConOracle()
    
    if df is None or df.empty:
        print("Failed to get data.")
        cursor.close()
        conn.close()
        return None

    sql = """
            UPDATE
                XININSURE.SALEACTION
            SET
                ACTIONSTATUS = 'Y'
            WHERE
                SALEID = :saleid
                AND ACTIONSTATUS NOT IN ('Y')
                AND "SEQUENCE" = :seq
        """

    try:

        for index, row in df.iterrows():
            cursor.execute(sql, {
                "saleid": row["SALEID"],
                "seq": row["SEQUENCE"]
            })

        #conn.commit()  
        return { 'Set_action_code': df }
    
    except Exception as e:
        print(f"Exception : {e}")
    finally:
        cursor.close()
        conn.close()

def Set_action_code(action_status = "X", request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้วไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ", df = pd.DataFrame()):
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

        #conn.commit()  
        return { 'Set_action_code': df }
    
    except oracledb.Error as e:
        print(f"Get_Data : {e}")
    finally:
        cursor.close()
        conn.close()
    
# Default arguments
default_args = {
    "owner": "DCP",
    "depends_on_past": False,
    "retries": 1,
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
            query = f"""
                        WITH SSA AS (
                                            SELECT
                                                SA.SALEID,
                                                SA.INSTALLMENT,
                                                A.ACTIONID,
                                                A.ACTIONCODE,
                                                SA.ACTIONSTATUS,
                                                SA.RESULTID,
                                                SA.DUEDATE,
                                                SA.ACTIONREMARK,
                                                SA.REQUESTREMARK,
                                                SA.SEQUENCE 
                                            FROM
                                                XININSURE.SALEACTION SA
                                                JOIN XININSURE.ACTION A ON SA.ACTIONID = A.ACTIONID 
                                            WHERE
                                                SA.ACTIONID IN (
                                                    2261,
                                                    3740,
                                                    3741,
                                                    3742,
                                                    3743,
                                                    3760,
                                                    3761,
                                                    5933,
                                                    7533,
                                                    9133,
                                                    9174,
                                                    11293,
                                                    11574,
                                                    11575,
                                                    11576,
                                                    11577,
                                                    11553,
                                                    11554,
                                                    11555,
                                                    15014 
                                                ) 
                                                AND SA.ACTIONSTATUS IN ( 'R', 'W', 'Y' ) 
                                    AND SA.DUEDATE BETWEEN TO_DATE ( '03/09/2023', 'DD/MM/YYYY' ) 
                                    AND TO_DATE ( '03/10/2023', 'DD/MM/YYYY' ) 
                                ),
                                PAID AS (
                                SELECT
                                    T.SALEID,
                                    SUM ( I.RECEIVEAMOUNT ) AS PAIDCURRENTDATE 
                                FROM
                                    XININSURE.RECEIVEITEMCLEAR T
                                    JOIN XININSURE.RECEIVEITEM I ON T.RECEIVEID = I.RECEIVEID 
                                    AND T.RECEIVEITEM = I.RECEIVEITEM
                                    JOIN XININSURE.RECEIVE R ON I.RECEIVEID = R.RECEIVEID 
                                WHERE
                                    R.RECEIVESTATUS IN ( 'S', 'C' ) 
                                    AND I.RECEIVEDATE BETWEEN TO_DATE ( '03/09/2023', 'DD/MM/YYYY' ) 
                                    AND TO_DATE ( '03/10/2023', 'DD/MM/YYYY' )
                                GROUP BY
                                    T.SALEID
                                ),
                                STOCK_RET AS ( SELECT ST.SALEID, MIN ( ST.RETURNDATE ) AS RETURNDATE FROM XININSURE.STOCK ST GROUP BY ST.SALEID ) SELECT
                                S.SALEID,
                                XININSURE.GETBOOKNAME ( S.PERIODID, S.SALEBOOKCODE, S.SEQUENCE ) AS BOOKNAME,
                                S.SALEBOOKCODE,
                                S.ROUTEID,
                                B.REGION,
                                R.ROUTECODE,
                                S.PAIDAMOUNT,
                                S.CANCELRESULTID,
                                ST.RETURNDATE,
                                SSA.ACTIONREMARK,
                                SSA.REQUESTREMARK,
                                SB.BYTECODE AS PAYMENTSTATUS,
                                S.PAYMENTMODE,
                                F.STAFFCODE,
                                F.STAFFTYPE,
                                F.STAFFCODE || ':' || F.STAFFNAME AS STAFFNAME,
                                D.DEPARTMENTID,
                                D.DEPARTMENTCODE || ':' || D.DEPARTMENTNAME AS DEPARTMENTNAME,
                                D.DEPARTMENTCODE,
                                D.DEPARTMENTGROUP,
                                SSA.ACTIONID,
                                SSA.ACTIONCODE,
                                SSA.ACTIONSTATUS,
                                SSA.SEQUENCE,
                                R.PROVINCECODE,
                                SSA.RESULTID,
                                RS.RESULTCODE,
                                S.MASTERSALEID,
                                SSA.DUEDATE,
                                P.PAIDCURRENTDATE,
                                PT.PRODUCTGROUP,
                                PT.PRODUCTTYPE, 
                                S.PRBSTATUS,
                                S.POLICYSTATUS,
                                S.SALESTATUS
                            FROM
                                XININSURE.sale S
                                JOIN SSA ON S.SALEID = SSA.SALEID
                                JOIN XININSURE.STAFF F ON S.STAFFID = F.STAFFID
                                JOIN XININSURE.DEPARTMENT D ON S.STAFFDEPARTMENTID = D.DEPARTMENTID
                                JOIN XININSURE.PRODUCT P ON S.PRODUCTID = P.PRODUCTID
                                JOIN XININSURE.PRODUCTTYPE PT ON P.PRODUCTTYPE = PT.PRODUCTTYPE
                                JOIN XININSURE.SUPPLIER SU ON P.SUPPLIERID = SU.SUPPLIERID
                                JOIN XININSURE.ROUTE R ON S.ROUTEID = R.ROUTEID
                                JOIN XININSURE.BRANCH B ON R.BRANCHID = B.BRANCHID
                                LEFT JOIN STOCK_RET ST ON S.SALEID = ST.SALEID
                                LEFT JOIN PAID P ON SSA.SALEID = P.SALEID
                                LEFT JOIN XININSURE.SYSBYTEDES SB ON SB.COLUMNNAME = 'PAYMENTSTATUS' 
                                AND SB.TABLENAME = 'SALE' 
                                AND SB.BYTECODE = S.PAYMENTSTATUS
                                LEFT JOIN XININSURE.RESULT RS ON RS.RESULTID = SSA.RESULTID 
                            WHERE
                                S.PLATEID IS NOT NULL 
                                AND S.CANCELDATE IS NULL 
                                AND S.CANCELEFFECTIVEDATE IS NULL
                                AND S.POLICYSTATUS = 'A'
                                OR S.PRBSTATUS = 'A'
                                AND S.SALESTATUS = 'O'
                            ORDER BY
                                S.SALEID DESC
 """
            
            print("Fetching data...")
            cursor.execute(query)
            df = pd.DataFrame(
                cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
            )

            print("======== start df original ============")
            print("row count:", len(df))
            print(df.head().to_markdown(index=False))
            print("======== end df original ============")
            
            df_digital_room = df.query("DEPARTMENTGROUP in ('DM')")
            
            df_notin_digital_room = df.query("DEPARTMENTGROUP not in ('DM')")
            
            # df_esy02 = df.query("ACTIONCODE in ('ESY02') and ACTIONSTATUS in ('Y')")
            
            formatted_table = df_notin_digital_room.to_markdown(index=False)
            # print(query)
            print(f"\n{formatted_table}")
            print(f"Get data successfully")
            print(f"df: {len(df_notin_digital_room)}")
            
            return { 'df_cancel_work': df_notin_digital_room, 'df_digital_room': df_digital_room }
        
        except oracledb.Error as e:
            print(f"Get_Data : {e}")
            
        finally:
            cursor.close()
            conn.close()          
                
    @task
    def Insert_digital_DM(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="get_cancellation_group.Get_cancelled_work", key="return_value")

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

                #conn.commit()  
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
        result = ti.xcom_pull(task_ids="get_cancellation_group.Get_cancelled_work", key="return_value")
        df = result.get("df_cancel_work", pd.DataFrame())

        print("==== df_cancel_work ====")
        print(df.head().to_markdown(index=False))

        if cursor is None or conn is None:
            return pd.DataFrame()

        try:
            if df.empty:
                print("DataFrame is empty.")
                return pd.DataFrame()

            saleids = [int(x) for x in df['SALEID'].unique()]
            if not saleids:
                return pd.DataFrame()

            batch_size = 5
            esy02_saleids = set()
            for i in range(0, len(saleids), batch_size):
                batch = saleids[i:i+batch_size]
                placeholders = ','.join([f':id{j}' for j in range(len(batch))])
                check_esy02_query = f"""       
                    WITH RankedActions AS (
                                            SELECT 
                                                S.SALEID,
                                                sa.SEQUENCE,
                                                ROW_NUMBER() OVER (
                                                    PARTITION BY S.SALEID 
                                                    ORDER BY sa.SEQUENCE DESC
                                                ) as rn
                                            FROM XININSURE.SALE S
                                            INNER JOIN XININSURE.SALEACTION sa ON S.SALEID = sa.SALEID
                                            INNER JOIN XININSURE.ACTION a ON sa.ACTIONID = a.ACTIONID
                                            WHERE S.SALEID IN ({placeholders})
                                                AND sa.ACTIONSTATUS = 'Y'
                                                AND a.ACTIONCODE = 'ESY02'
                                                AND S.PLATEID IS NOT NULL 
                                                AND S.CANCELDATE IS NULL 
                                                AND S.CANCELEFFECTIVEDATE IS NULL
                                                AND (S.POLICYSTATUS = 'A' OR S.PRBSTATUS = 'A')
                                                AND S.SALESTATUS = 'O'
                                        )
                                        SELECT SALEID 
                                        FROM RankedActions 
                                        WHERE rn = 1
                """
                params = {f'id{j}': int(v) for j, v in enumerate(batch)}
                cursor.execute(check_esy02_query, params)
                esy02_saleids.update(row[0] for row in cursor.fetchall())

            # 2. แบ่งกลุ่มใน Pandas
            df_result_is_esy = df[df['SALEID'].isin(esy02_saleids)].copy()
            df_result_not_esy = df[~df['SALEID'].isin(esy02_saleids)].copy()

            print("==== df_result_is_esy ====")
            print(df_result_is_esy.head().to_markdown(index=False))
            print("==== end df_result_is_esy ====")

            if not df_result_is_esy.empty and "RESULTCODE" in df_result_is_esy.columns:
                df_filter_esy_noresultcode = df_result_is_esy.query("RESULTCODE not in ('XPOL', 'XALL', 'XPRB')")
                df_filter_esy_resultcode = df_result_is_esy.query("RESULTCODE in ('XPOL', 'XALL', 'XPRB')")
            else:
                df_filter_esy_noresultcode = pd.DataFrame()
                df_filter_esy_resultcode = pd.DataFrame()

            if not df_result_not_esy.empty and "RESULTCODE" in df_result_not_esy.columns:
                df_filter_notesy_noresultcode = df_result_not_esy.query("RESULTCODE not in ('XPOL', 'XALL', 'XPRB')")
                df_filter_notesy_resultcode = df_result_not_esy.query("RESULTCODE in ('XPOL', 'XALL', 'XPRB')")
            else:
                df_filter_notesy_noresultcode = pd.DataFrame()
                df_filter_notesy_resultcode = pd.DataFrame()

            formatted_table = df.to_markdown(index=False)
            print(f"\n{formatted_table}")
            print(f"Select ESY02 data successfully")
            print(f"Total records found: {len(df)}")
            print(f"Total SALEIDs checked: {len(df)}")
            print(f"Total is esy no_result_code = {df_filter_esy_noresultcode}")

            print(f"Total is esy has_result_code = {df_filter_esy_resultcode}")
            print(f"Total not esy no_result_code = {df_filter_notesy_noresultcode}")
            print(f"Total not esy has_result_code = {df_filter_notesy_resultcode}")

            request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้วไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ"
            action_status = "X"

            # conn.commit()

            return {
                'action_status': action_status,
                'request_remark': request_remark,
                'df_filter_esy_noresultcode': df_filter_esy_noresultcode,
                'df_filter_esy_resultcode': df_filter_esy_resultcode,
                'df_filter_notesy_noresultcode': df_filter_notesy_noresultcode,
                'df_filter_notesy_resultcode': df_filter_notesy_resultcode
            }

        except oracledb.Error as error:
            conn.rollback()
            message = f'เกิดข้อผิดพลาด : {error}'
            print(message)
            return pd.DataFrame()
        finally:
            cursor.close()
            conn.close()

    @task
    def process_esy_no_result_code(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df = result.get("df_filter_esy_noresultcode", pd.DataFrame()) # easy no result code

        print("============ process_esy_no_result_code start ============")

        if df.empty:
            print("ESY No : records to process.")
            return None
        print("isin invalid action codes: ", invalid_action_codes)
        print("type of", invalid_action_codes, "is", type(invalid_action_codes))
        df_invalid_actions = df[df['ACTIONCODE'].isin(invalid_action_codes)]
        df_valid_actions = df[~df['ACTIONCODE'].isin(invalid_action_codes)]
        
                
        if not df_valid_actions.empty:
            action_status = "X"
            request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้ว ไม่สามารถยกเลิกได้ รบกวนตรวจสอบค่ะ"
            print("============ df_invalid_actions start ============")
            print(df_valid_actions.head().to_markdown(index=False))
            Set_action_code(action_status, request_remark, df)
        
        if not df_invalid_actions.empty:
            action_status = "W"
            request_remark = "Auto Cancel MT สินเชื่อ ESY อนุมัติแล้ว ไม่สามารถยกเลิกได้ รบกวนตรวจสอบครับ"
            print("============ df_invalid_actions start ============")
            print(df_invalid_actions.head().to_markdown(index=False))
            Set_action_code(action_status, request_remark, df)

        return None
    
    @task
    def process_not_esy_no_result_code(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df = result.get("df_filter_notesy_noresultcode", pd.DataFrame()) # not easy no result code

        print("============ process_not_esy_no_result_code start ============")

        if df.empty:
            print("NOT ESY : No records to process.")
            return None
        
        df_invalid_actions = df[df['ACTIONCODE'].isin(invalid_action_codes)]
        df_valid_actions = df[~df['ACTIONCODE'].isin(invalid_action_codes)]

        if df_valid_actions.empty:
            action_status = "X"
            request_remark = "Auto Cancel MT รหัสผลที่กำหนดไม่ถูกต้อง ไม่สามารถดำเนินการยกเลิกได้ รบกวนตรวจสอบค่ะ"
            print("============ df_invalid_actions start ============")
            print(df_valid_actions.head().to_markdown(index=False))
            Set_action_code(action_status, request_remark, df)
            print("============ df_invalid_actions end ============")

        if df_invalid_actions.empty:
            action_status = "W"
            request_remark = "Auto Cancel MT รหัสผลที่กำหนดไม่ถูกต้อง ไม่สามารถดำเนินการยกเลิกได้ รบกวนตรวจสอบค่ะ"
            print("============ df_invalid_actions start ============")
            print(df_valid_actions.head().to_markdown(index=False))
            Set_action_code(action_status, request_remark, df)
            print("============ df_invalid_actions end ============")

        return None
    
    @task
    def Check_balance(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df = result.get("df_filter_esy_resultcode", pd.DataFrame())
        # df_esy_noresult = result.get("df_filter_esy_noresultcode", pd.DataFrame())
        
        empty_df_result = pd.DataFrame(columns=df.columns) if df is not None and not df.empty else pd.DataFrame()
        # empty_df_noresult = pd.DataFrame(columns=df_esy_noresult.columns) if df_esy_noresult is not None and not df_esy_noresult.empty else pd.DataFrame()
        
        try:
            # df = pd.concat([df_esy_result, df_esy_noresult], ignore_index=True)
            print(f"df: {len(df)}")

            mask = df["PAIDCURRENTDATE"] > 0 if not df.empty and "PAIDCURRENTDATE" in df.columns else pd.Series(dtype=bool)
            df_has_paid = df[mask] if not df.empty else empty_df_result
            df_no_paid = df[~mask] if not df.empty else empty_df_result
            
            return {"df_has_paid":df_has_paid,"df_no_paid":df_no_paid}
        
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass
        
    @task
    def Check_package_balance(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Check_balance", key="return_value")
        df_has_paid = result["df_has_paid"]
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Check_balance", key="return_value")
        df_no_paid = result["df_no_paid"]
        
        empty_df_has_paid = pd.DataFrame(columns=df_has_paid.columns) if df_has_paid is not None and not df_has_paid.empty else pd.DataFrame()
        empty_df_no_paid = pd.DataFrame(columns=df_no_paid.columns) if df_no_paid is not None and not df_no_paid.empty else pd.DataFrame()
        try:
            
            df_paymentstatus_Y = df_has_paid.query("PAYMENTSTATUS == 'Y'") if df_has_paid is not None and not df_has_paid.empty else empty_df_has_paid
            df_paymentstatus_not_Y = df_no_paid.query("PAYMENTSTATUS not in 'Y'") if df_no_paid is not None and not df_no_paid.empty else empty_df_no_paid
            
            formatted_table_df_paymentstatus_Y = df_paymentstatus_Y.to_markdown(index=False)
            formatted_table_df_paymentstatus_not_Y = df_paymentstatus_not_Y.to_markdown(index=False)
            
            # print(query)
            print(f"\n{formatted_table_df_paymentstatus_Y}")
            print(f"\n{formatted_table_df_paymentstatus_not_Y}")
            
            return {"df_paymentstatus_Y":df_paymentstatus_Y, "df_paymentstatus_not_Y":df_paymentstatus_not_Y}
        
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass
        
    @task
    def Split_segment_condition(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Check_package_balance", key="return_value")
        df_paymentstatus_Y = result["df_paymentstatus_Y"]
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Check_balance", key="return_value")
        df_no_paid = result["df_no_paid"]
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Check_balance", key="return_value")
        df_has_paid = result["df_has_paid"]
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df_filter_notesy_noresultcode = result["df_filter_notesy_noresultcode"]

        try:
            # ไม่มียอดชำระ ชำระครบ ไม่เป็นงาน esy และมี resultcode เป็น (XALL, XPOL, XPRB)
            df_XALL_Y = df_paymentstatus_Y.query("ACTIONCODE == 'XALL'") if df_paymentstatus_Y is not None and not df_paymentstatus_Y.empty else pd.DataFrame()
            df_XPOL_Y = df_paymentstatus_Y.query("ACTIONCODE == 'XPOL'") if df_paymentstatus_Y is not None and not df_paymentstatus_Y.empty else pd.DataFrame()
            
            # resultcode เป็น XPRB และ ไม่มียอดรับชำระ
            df_XPRB_W = df_no_paid.query("ACTIONCODE == 'XPRB'") if df_no_paid is not None and not df_no_paid.empty else pd.DataFrame()
            
            # มียอดชำระและมี resultcode เป็น (XALL, XPOL, XPRB)
            df_has_paid_XALL = df_has_paid.query("ACTIONCODE == 'XALL'") if df_has_paid is not None and not df_has_paid.empty else pd.DataFrame()
            df_has_paid_XPOL = df_has_paid.query("ACTIONCODE == 'XPOL'") if df_has_paid is not None and not df_has_paid.empty else pd.DataFrame()
            df_has_paid_XPRB = df_has_paid.query("ACTIONCODE == 'XPRB'") if df_has_paid is not None and not df_has_paid.empty else pd.DataFrame()
            df_concat_resultcode_has_paid = pd.concat([df_has_paid_XALL, df_has_paid_XPOL, df_has_paid_XPRB], ignore_index=True) if not (df_has_paid_XALL.empty and df_has_paid_XPOL.empty and df_has_paid_XPRB.empty) else pd.DataFrame()
                        
            # resultcode เป็น XPRB และมีหมายเลขกรมธรรม์
            df_XPRB_has_policynumber = df_XPRB_W[df_XPRB_W["PRBPOLICYNUMBER"].notnull()] if not df_XPRB_W.empty else pd.DataFrame()
            
            # resultcode เป็น XPRB และไม่มีหมายเลขกรมธรรม์
            df_XPRB_no_policynumber = df_XPRB_W[df_XPRB_W["PRBPOLICYNUMBER"].isnull()] if not df_XPRB_W.empty else pd.DataFrame()
            
            # เตรียมนำไปกรองข้อมูลเฉพาะ return date
            df_concat_resultcode = pd.concat([df_XALL_Y, df_XPOL_Y, df_XPRB_no_policynumber], ignore_index=True) if not (df_XALL_Y.empty and df_XPOL_Y.empty and df_XPRB_no_policynumber.empty) else pd.DataFrame()
            
            mask = df_concat_resultcode["RETURNDATE"].notnull() if not df_concat_resultcode.empty else pd.Series(dtype=bool)

            # กรองข้อมูลที่มีค่า return date
            df_concat_resultcode_has_returndate = df_concat_resultcode[mask] if not df_concat_resultcode.empty else pd.DataFrame()
            
            # กรองข้อมูลที่ไม่มีค่า return date
            df_concat_resultcode_no_returndate = df_concat_resultcode[~mask] if not df_concat_resultcode.empty else pd.DataFrame()

            return {
                "df_concat_resultcode_has_returndate": df_concat_resultcode_has_returndate,
                "df_concat_resultcode_no_returndate": df_concat_resultcode_no_returndate,
                "df_XPRB_has_policynumber": df_XPRB_has_policynumber,
                "df_XPRB_no_policynumber": df_XPRB_no_policynumber,
                "df_concat_resultcode_has_paid": df_concat_resultcode_has_paid
            }
        
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass
        
    @task
    def Split_has_remark(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        # ดึงข้อมูลจาก task Split_segment_condition
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df = result.get("df_filter_notesy_resultcode", pd.DataFrame())
        
        # อ่าน cancel_messages จาก config
        cancel_messages = json.loads(cancel_messages_str)
        
        try:
            if df.empty:
                print("No data to process in Split_has_remark")
                return {"matched_df": pd.DataFrame(), "unmatched_df": pd.DataFrame()}
            
            # สร้าง dictionary เพื่อเก็บผลลัพธ์
            result_dfs = {}
            unmatched_df = pd.DataFrame()
            matched_df = pd.DataFrame()
            
            
            # Return แยก matched และ unmatched DataFrame
            return {
                "matched_df": matched_df,
                "unmatched_df": unmatched_df,
                "result_by_code": result_dfs  # เก็บไว้สำหรับใช้งานแยกตามรหัส (ถ้าต้องการ)
            }
            
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Split_has_remark : {e}")
            return {"matched_df": pd.DataFrame(), "unmatched_df": pd.DataFrame(), "result_by_code": {}}
        
    @task
    def Condition_B(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df_filter_notesy_noresultcode = result.get("df_filter_notesy_noresultcode", pd.DataFrame())
        
        formatted_table_df_filter_notesy_noresultcode = df_filter_notesy_noresultcode.to_markdown(index=False)
        
        print(f"\n{formatted_table_df_filter_notesy_noresultcode}")
        
        try:
            # ไม่เป็นรหัสผลยกเลิกที่กำหนด เช่น (XALL, XPOL, XPRB)
            if df_filter_notesy_noresultcode is not None and not df_filter_notesy_noresultcode.empty:
                action_status = "W"
                request_remark = "Auto Cancel MT รหัสผลที่กำหนดไม่ถูกต้อง ไม่สามารถดำเนินการยกเลิกได้ รบกวนตรวจสอบค่ะ"
                Set_action_code(action_status, request_remark, df_filter_notesy_noresultcode)
            return True
        
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass
            
    @task
    def Condition_C(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        result = ti.xcom_pull(task_ids="get_cancellation_group.Split_segment_condition", key="return_value")
        df_concat_resultcode_has_paid = result.get("df_concat_resultcode_has_paid", pd.DataFrame())
        df_concat_resultcode_no_returndate = result.get("df_concat_resultcode_no_returndate", pd.DataFrame())
        
        formatted_table_df_concat_resultcode_has_paid = df_concat_resultcode_has_paid.to_markdown(index=False)
        formatted_table_df_concat_resultcode_no_returndate = df_concat_resultcode_no_returndate.to_markdown(index=False)
            
        # print(query)
        print(f"\n{formatted_table_df_concat_resultcode_has_paid}")
        
        print(f"\n{formatted_table_df_concat_resultcode_no_returndate}")
        
        try:
            # resultcode ที่อยู่ใน (XALL, XPOL, XPRB) และ มียอดชำระเข้ามาในวัน
            if df_concat_resultcode_has_paid is not None and not df_concat_resultcode_has_paid.empty:
                action_status = "X"
                request_remark = "Auto Cancel MT พบยอดรับชำระหลังจากตั้งโค้ดยกเลิก หากต้องการยกเลิกรบกวนตั้งโค้ดยกเลิกให้ใหม่อีกครั้ง"
                Set_action_code(action_status, request_remark, df_concat_resultcode_has_paid)
            # resultcode ที่อยูใน (XALL, XPOL, XPRB) และ ไม่มีค่า Returndate
            elif df_concat_resultcode_no_returndate is not None and not df_concat_resultcode_no_returndate.empty:
                action_status = "W"
                request_remark = "Auto Cancel MT ระบบไม่สามารถยกเลิกได้ รบกวนตรวจสอบ"
                Set_action_code(action_status, request_remark, df_concat_resultcode_no_returndate)
            return True
        
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass
        
    @task
    def Split_has_remark(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        # ดึงข้อมูลจาก task Split_segment_condition
        result = ti.xcom_pull(task_ids="get_cancellation_group.Select_esy02_X", key="return_value")
        df = result.get("df_filter_notesy_resultcode", pd.DataFrame())
        
        # อ่าน cancel_messages จาก config
        cancel_messages = json.loads(cancel_messages_str)
        
        try:
            if df.empty:
                print("No data to process in Split_has_remark")
                return {"matched_df": pd.DataFrame(), "unmatched_df": pd.DataFrame()}
            
            # สร้าง dictionary เพื่อเก็บผลลัพธ์
            result_dfs = {}
            unmatched_df = pd.DataFrame()
            matched_df = pd.DataFrame()
            
            for index, row in df.iterrows():
                # ตรวจสอบ REQUESTREMARK ก่อน ถ้าไม่มีจึงดู ACTIONREMARK
                remark_text = ""
                if pd.notna(row.get("REQUESTREMARK")) and str(row.get("REQUESTREMARK")).strip():
                    remark_text = str(row.get("REQUESTREMARK")).strip()
                elif pd.notna(row.get("ACTIONREMARK")) and str(row.get("ACTIONREMARK")).strip():
                    remark_text = str(row.get("ACTIONREMARK")).strip()
                
                if not remark_text:
                    # ไม่มี remark ให้เพิ่มเข้า unmatched
                    if unmatched_df.empty:
                        unmatched_df = row.to_frame().T
                    else:
                        unmatched_df = pd.concat([unmatched_df, row.to_frame().T], ignore_index=True)
                    continue
                
                # หารหัสสาเหตุยกเลิกที่ตรงกัน
                matched_code = None
                for code, messages in cancel_messages.items():
                    for message in messages:
                        if message in remark_text:
                            matched_code = code
                            break
                    if matched_code:
                        break
                
                if matched_code:
                    # เพิ่มข้อมูลเข้า matched_df
                    if matched_df.empty:
                        matched_df = row.to_frame().T
                    else:
                        matched_df = pd.concat([matched_df, row.to_frame().T], ignore_index=True)
                    
                    # เพิ่มข้อมูลเข้า DataFrame ที่ตรงกับรหัสสาเหตุ (เก็บไว้สำหรับแสดงผล)
                    if matched_code not in result_dfs:
                        result_dfs[matched_code] = pd.DataFrame()
                    
                    if result_dfs[matched_code].empty:
                        result_dfs[matched_code] = row.to_frame().T
                    else:
                        result_dfs[matched_code] = pd.concat([result_dfs[matched_code], row.to_frame().T], ignore_index=True)
                    
                    print(f"SALEID: {row.get('SALEID')} matched with code: {matched_code} for remark: {remark_text}")
                else:
                    # ไม่ตรงกับรหัสใดเลย
                    if unmatched_df.empty:
                        unmatched_df = row.to_frame().T
                    else:
                        unmatched_df = pd.concat([unmatched_df, row.to_frame().T], ignore_index=True)
                    print(f"SALEID: {row.get('SALEID')} - No matching code found for remark: {remark_text}")
            
            # แสดงสรุปผลลัพธ์
            for code, df_result in result_dfs.items():
                print(f"Code {code}: {len(df_result)} records")
                if not df_result.empty:
                    formatted_table = df_result.to_markdown(index=False)
                    print(f"\n{formatted_table}")
            
            print(f"Total matched records: {len(matched_df)}")
            print(f"Total unmatched records: {len(unmatched_df)}")
            
            # Return แยก matched และ unmatched DataFrame
            return {
                "matched_df": matched_df,
                "unmatched_df": unmatched_df,
                "result_by_code": result_dfs  # เก็บไว้สำหรับใช้งานแยกตามรหัส (ถ้าต้องการ)
            }
            
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Split_has_remark : {e}")
            return {"matched_df": pd.DataFrame(), "unmatched_df": pd.DataFrame(), "result_by_code": {}}

    @task.branch
    def Check_time_result_cancel(**kwargs):
        current_time = datetime.now(local_tz).time()
        flg_time = datetime.strptime("15:00", "%H:%M").time()

        if (current_time < flg_time):
            return "condition_group.result_cancel_before_3pm"
        else:
            return "condition_group.result_cancel_after_3pm"

    @task
    def result_cancel_before_3pm(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="get_cancellation_group.Split_segment_condition", key="return_value")
        df = result.get("result_cancel_before_3pm", pd.DataFrame()) # #FF0000 ต้องมาเปลี่ยนทีหลัง

        print("======= start result_cancel_before_3pm =======")
        
        try:
            if df.empty:
                print("No ESY records to process for cancellation before 3 PM.")
                return None
            Set_result_cancel(df, isBefore3PM=True)
            Set_sale_action_status(df)
        except Exception as e:
            print(f"Exception in result_cancel_before_3pm: {e}")
        return None
    
    @task
    def result_cancel_after_3pm(**kwargs):
        ti = kwargs["ti"]
        result = ti.xcom_pull(task_ids="get_cancellation_group.Split_segment_condition", key="return_value")
        df = result.get("result_cancel_before_3pm", pd.DataFrame()) # #FF0000 ต้องมาเปลี่ยนทีหลัง

        print("======= start result_cancel_after_3pm =======")
        try:
            if df.empty:
                print("No ESY records to process for cancellation after 3 PM.")
                return None
            Set_result_cancel(df, isBefore3PM=False)
            Set_sale_action_status(df)
        except Exception as e:
            print(f"Exception in result_cancel_after_3pm: {e}")
        return None
    
    @task
    def Split_update_v(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        print(f"{message}")
        
        result = ti.xcom_pull(task_ids="condition_group.Split_has_remark", key="return_value")
        df = result["matched_df"]

        try:
            # ไม่มียอดชำระ ชำระครบ ไม่เป็นงาน esy และมี resultcode เป็น (XALL, XPOL, XPRB)
            df_XPRB_V = df.query("ACTIONCODE == 'XPRB' and PRBSTATUS == 'C' and POLICYSTATUS == 'C'") if df is not None and not df.empty else pd.DataFrame()
            
            # resultcode เป็น XALL และ ไม่มียอดรับชำระ
            df_XPOL_V = df.query("ACTIONCODE == 'XPOL' and PRBSTATUS == 'C' and POLICYSTATUS == 'C'") if df is not None and not df.empty else pd.DataFrame()
            
            # resultcode เป็น XALL และ ไม่มียอดรับชำระ
            df_XALL_V = df.query("ACTIONCODE == 'XALL'") if df is not None and not df.empty else pd.DataFrame()
            
            df_update_V = pd.concat([df_XALL_V, df_XPOL_V, df_XPRB_V], ignore_index=True) if not (df_XALL_V.empty and df_XPOL_V.empty and df_XPRB_V.empty) else pd.DataFrame()

            return {
                "df_update_V": df_update_V,
            }
        
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass
    
    @task
    def update_salestatus_V(**kwargs):
        ti = kwargs["ti"]
        task_id = kwargs['task_instance'].task_id
        try_number = kwargs['task_instance'].try_number
        message = f"Processing task {task_id} ,try_number {try_number}"
        # print(f"{message}")
        # send_flex_notification_start(message)
        
        result = ti.xcom_pull(task_ids="condition_group.Split_update_v", key="return_value")
        
        df = result["df_update_V"]
        
        cursor, conn = ConOracle()
        
        try:
            if df is None or df.empty:
                print("DataFrame is empty. Exiting task.")
                return {"Update_v_sum": df}
            else:           
                update_status_query_V = """
                        UPDATE XININSURE.SALE a
                        SET a.SALESTATUS = 'V'
                        WHERE a.SALEID = :SALEID
                        AND a.SALESTATUS = 'O'
                        AND a.PRBSTATUS = 'C'
                        AND a.POLICYSTATUS = 'C'
                    """
                    
                i = 0
                for index, row in df.iterrows():
                    cursor.execute(update_status_query_V, {'SALEID': row['SALEID']})
                    print(f"Number: {i+1} Updated Salestatus to N row {index}: SALEID={row['SALEID']}")
                    i+=1
                
                # conn.commit() 
                print("All updates committed successfully.")
                formatted_table = df.to_markdown(index=False)
                print(f"\n{formatted_table}")
                message = f"จำนวนรายการที่ปรับสถานะรวม {df} รายการ"
                print(message)
                return {"Update_v_sum": df}
        except Exception as e:
            message = f"Fail with task {task_id} \n error : {e}"
            print(f"Check_payment_date : {e}")
            pass

    #Dummy
    start = EmptyOperator(task_id="start_dag", trigger_rule="none_failed_min_one_success")
    end = EmptyOperator(task_id="end_dag", trigger_rule="none_failed_min_one_success")
    holiday_path = EmptyOperator(task_id="Holiday_path", trigger_rule="none_failed_min_one_success")
    work_path = EmptyOperator(task_id="Work_path", trigger_rule="none_failed_min_one_success")
    join_x = EmptyOperator(task_id="join_x_branch", trigger_rule="none_failed_min_one_success")
    join_v = EmptyOperator(task_id="join_v_branch", trigger_rule="none_failed_min_one_success")
    join_w = EmptyOperator(task_id="join_w_branch", trigger_rule="none_failed_min_one_success")
    # execute_x = EmptyOperator(task_id="execute_x_path", trigger_rule="none_failed_min_one_success")
    execute_v = EmptyOperator(task_id="execute_v_path", trigger_rule="none_failed_min_one_success")
    # process_esy_task = EmptyOperator(task_id="process_esy_task", trigger_rule="none_failed_min_one_success")
    # process_not_esy_task = EmptyOperator(task_id="process_not_esy_task", trigger_rule="none_failed_min_one_success")
    # combine_task = EmptyOperator(task_id="combine_task", trigger_rule="none_failed_min_one_success")
    # get_cancellation_work = EmptyOperator(task_id="get_cancellation_work", trigger_rule="none_failed_min_one_success")
    
    #task ที่ไป call function
    check_holiday_task = check_holiday()
    
    @task_group(group_id="get_cancellation_group")
    def get_cancellation_group():
        get_cancelltaion_work = Get_cancelled_work()
        get_esy02_X_task = Select_esy02_X()
        insert_digital_DM_task = Insert_digital_DM()
        Check_balance_task = Check_balance()
        Check_package_balance_task = Check_package_balance()
        Split_segment_condition_task = Split_segment_condition()
        
        get_cancelltaion_work >> get_esy02_X_task >> insert_digital_DM_task >> Check_balance_task >> Check_package_balance_task >> Split_segment_condition_task
        
    @task_group(group_id="process_x_group")
    def process_x_group():
        process_esy_task = process_esy_no_result_code()
        process_not_esy_task = process_not_esy_no_result_code()
        
        [process_esy_task, process_not_esy_task]

    @task_group(group_id="condition_group")  
    def condition_group():
        condition_c_task = Condition_C()
        condition_b_task = Condition_B()
        Split_has_remark_task = Split_has_remark()
        Split_update_v_task = Split_update_v()
        update_salestatus_V_task = update_salestatus_V()
        check_time_result_task = Check_time_result_cancel()
        before_3pm_task = result_cancel_before_3pm()
        after_3pm_task = result_cancel_after_3pm()
        
        condition_c_task >> condition_b_task >> Split_has_remark_task >> Split_update_v_task >> update_salestatus_V_task >> check_time_result_task >> [before_3pm_task, after_3pm_task]
        
    #กำหนด workflow
    (
        #เริ่มต้น
        start >> check_holiday_task >> [holiday_path, work_path],
        holiday_path >> end,
        
        #ระงับยกเลิกไป join x , ยกเลิกไป join v
        work_path >> get_cancellation_group()
        >> [join_x, join_v, join_w],
        
        # แทนค่าด้วยฟังก์ชันระงับยกเลิกได้เลย
        join_x >> process_x_group() >> end,
        
        join_v >> execute_v >> end,
        
        join_w >> condition_group() >> end,
    )