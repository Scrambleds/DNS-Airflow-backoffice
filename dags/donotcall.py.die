# import configparser
# # from airflow import DAG
# # from airflow.decorators import dag, task, task_group  # เพิ่ม import นี้
# # from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow import DAG
# from airflow.decorators import task, dag, task_group
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timezone, timedelta
# import pandas as pd
# import numpy as np
# import oracledb
# from pythainlp.util import thai_strftime
# from airflow.utils.dates import days_ago
# # import pytz
# import requests
# import pendulum
# import os
# from os import path
# import base64
# import io
# import logging
# # from dotenv import find_dotenv, load_dotenv
# # from powered import powered_by_interns
# from pythainlp.util import thai_strftime
# from datetime import datetime
# import copy
# import time
# from linebot import LineBotApi
# from linebot.exceptions import LineBotApiError
# from linebot.models import TextSendMessage
# from linebot.models import FlexSendMessage
# import cx_Oracle

# config_file_path = 'config/db_config.cfg'
# config = configparser.ConfigParser()
# config.read(config_file_path)
# local = config.get("variable","tz")
# local_tz = pendulum.timezone(local)
# currentDateAndTime = datetime.now(tz=local_tz) 

# # วันที่และเวลาปัจจุบัน
# currentDate = currentDateAndTime.strftime("%Y-%m-%d")
# currentTime = currentDateAndTime.strftime("%H:%M:%S")

# line_user_id_user = config.get('variable', 'line_user_id_user')
# line_access_token_user = config.get('variable', 'channel_access_token_user')

# line_user_id = config.get('variable', 'line_user_id')
# # line_channel_secret = config.get('variable', 'channel_secret')
# line_access_token = config.get('variable', 'channel_access_token')

# # ตั้งค่า Line Bot API (ควรทำครั้งเดียวตอนเริ่มต้น)
# line_bot_api = LineBotApi(config.get('variable', 'channel_access_token'))
# line_bot_api_user = LineBotApi(config.get('variable', 'channel_access_token_user'))
# dnc_dates = config.get('variable', 'dnc_dates')

# def send_line_notification(message, user_id=line_user_id):
#     """
#     ส่งข้อความผ่าน Line Messaging API
#     :param message: ข้อความที่จะส่ง
#     :param user_id: Line User ID ของผู้รับ (ถ้าไม่ระบุจะใช้ค่า default)
#     """
#     try:
#         # สร้าง LineBotApi instance ด้วย access token
#         line_bot_api = LineBotApi(line_access_token)
        
#         # ส่งข้อความ
#         line_bot_api.push_message(
#             user_id,
#             TextSendMessage(text=message)
#         )
#         logging.info("Line notification sent successfully")
#         return True
#     except LineBotApiError as e:
#         logging.error(f"Line API error: {e}")
#         return False
#     except Exception as e:
#         logging.error(f"Error sending Line notification: {e}")
#         return False
    
# def send_flex_notification_start(message=None):
#     """
#     ส่ง Flex Message เริ่มต้นทำงาน
#     :param message: ข้อความเพิ่มเติมในกรณี error (optional)
#     """
#     try:
#         contents = {
#             "type": "bubble",
#             "body": {
#                 "type": "box",
#                 "layout": "vertical",
#                 "contents": [
#                     {
#                         "type": "text",
#                         "text": "AUTOMATION",
#                         "color": "#00d610",
#                         "size": "sm",
#                         "weight": "bold",
#                         "margin": "xs"
#                     },
#                     {
#                         "type": "text",
#                         "text": "ระบบเรียกงาน DNC",
#                         "size": "lg",
#                         "margin": "xl",
#                         "color": "#727272",
#                         "weight": "bold"
#                     },
#                     {
#                         "type": "text",
#                         "text": "[ระบบเริ่มทำงาน]" if not message else "[เกิดข้อผิดพลาด]",
#                         "margin": "none",
#                         "size": "lg",
#                         "weight": "bold",
#                         "color": "#727272"
#                     },
#                     {
#                         "type": "text",
#                         "text": f"วันที่ {currentDate} เวลา {currentTime} น.",
#                         "margin": "lg",
#                         "size": "xs",
#                         "color": "#a6a6a6"
#                     },
#                     {
#                         "type": "separator",
#                         "margin": "lg"
#                     }
#                 ]
#             }
#         }
        
#         if message:
#             contents["body"]["contents"].append({
#                 "type": "text",
#                 "text": "รายละเอียดข้อผิดพลาด:",
#                 "margin": "lg",
#                 "size": "md",
#                 "weight": "bold",
#                 "color": "#000000"
#             })
#             contents["body"]["contents"].append({
#                 "type": "text",
#                 "text": message,
#                 "margin": "md",
#                 "size": "sm",
#                 "color": "#000000",
#                 "wrap": True
#             })
        
#         # สร้าง Flex Message
#         flex_message = FlexSendMessage(
#             alt_text="สถานะการทำงาน DNC",
#             contents=contents
#         )

#         # ส่งข้อความ
#         # line_bot_api = LineBotApi(line_access_token)
#         line_bot_api.push_message(line_user_id, flex_message)
        
#         logging.info("Flex notification sent successfully")
#         return True
        
#     except Exception as e:
#         logging.error(f"Error sending Flex notification: {e}")
#         return False
    
# def send_flex_notification_end(qccode_results, user_id=line_user_id_user):
#     """
#     ส่ง Flex Message สรุปผลผ่าน Line Messaging API
#     :param qccode_results: ผลลัพธ์จาก Split_qccode_dnc
#     :param user_id: Line User ID ของผู้รับ
#     """
#     try:
#         # สร้างเนื้อหา Flex Message
#         contents = {
#             "type": "bubble",
#             "body": {
#                 "type": "box",
#                 "layout": "vertical",
#                 "contents": [
#                     {
#                         "type": "text",
#                         "text": "AUTOMATION",
#                         "color": "#00d610",
#                         "size": "sm",
#                         "weight": "bold",
#                         "margin": "xs"
#                     },
#                     {
#                         "type": "text",
#                         "text": "ระบบเรียกงาน DNC",
#                         "size": "lg",
#                         "margin": "xl",
#                         "color": "#727272",
#                         "weight": "bold"
#                     },
#                     {
#                         "type": "text",
#                         "text": "[ระบบทำงานเสร็จสิ้น]",
#                         "margin": "none",
#                         "size": "lg",
#                         "weight": "bold",
#                         "color": "#727272"
#                     },
#                     {
#                         "type": "text",
#                         "text": f"วันที่ {currentDate} เวลา {currentTime} น.",
#                         "margin": "lg",
#                         "size": "xs",
#                         "color": "#a6a6a6"
#                     },
#                     {
#                         "type": "separator",
#                         "margin": "lg"
#                     },
#                     {
#                         "type": "text",
#                         "text": "รายการครบกำหนดสิ้นสุดระยะติดต่อ",
#                         "margin": "lg",
#                         "size": "md",
#                         "weight": "bold",
#                         "color": "#000000"
#                     }
#                 ]
#             }
#         }

#         # เพิ่มข้อมูลแต่ละ QCCODE
#         body_contents = contents["body"]["contents"]
        
#         # คำนวณจำนวนงานทั้งหมด
#         total_count = (
#             len(qccode_results.get("df_DNC", pd.DataFrame())) +
#             len(qccode_results.get("df_V2T", pd.DataFrame())) +
#             len(qccode_results.get("df_AI", pd.DataFrame())) +
#             len(qccode_results.get("df_MISMATCH_NUM", pd.DataFrame()))
#         )

#         # เพิ่มสรุปจำนวนงานทั้งหมด
#         body_contents.append({
#             "type": "box",
#             "layout": "horizontal",
#             "contents": [
#                 {
#                     "type": "text",
#                     "text": "รวมทั้งสิ้น",
#                     "size": "md",
#                     "color": "#333333",
#                     "align": "start",
#                     "flex": 2
#                 },
#                 {
#                     "type": "text",
#                     "text": f"{total_count} รายการ",
#                     "size": "md",
#                     "color": "#333333",
#                     "align": "end",
#                     "flex": 1
#                 }
#             ],
#             "margin": "md"
#         })

#         # เพิ่มเส้นคั่น
#         body_contents.append({
#             "type": "separator",
#             "margin": "lg"
#         })

#         # เพิ่มหัวข้อสรุปผลการปรับสถานะ
#         body_contents.append({
#             "type": "text",
#             "text": "สรุปผลการปรับสถานะ",
#             "margin": "lg",
#             "size": "md",
#             "weight": "bold",
#             "color": "#000000"
#         })

#         def add_summary_row(title, count):
#             body_contents.append({
#                 "type": "box",
#                 "layout": "horizontal",
#                 "contents": [
#                     {
#                         "type": "text",
#                         "text": f"{title}",
#                         "size": "md",
#                         "color": "#333333",
#                         "align": "start",
#                         "flex": 2
#                     },
#                     {
#                         "type": "text",
#                         "text": f"{count} รายการ",
#                         "size": "md",
#                         "color": "#333333",
#                         "align": "end",
#                         "flex": 1
#                     }
#                 ],
#                 "margin": "md"
#             })

#         add_summary_row("Do not call list (10)", len(qccode_results.get("df_DNC", pd.DataFrame())))
#         add_summary_row("V2T (11)", len(qccode_results.get("df_V2T", pd.DataFrame())))
#         add_summary_row("AI-Do not call (12)", len(qccode_results.get("df_AI", pd.DataFrame())))
#         add_summary_row("เบอร์ผิด (21)", len(qccode_results.get("df_MISMATCH_NUM", pd.DataFrame())))

#         # เพิ่มส่วนท้าย
#         body_contents.extend([
#             {
#                 "type": "separator",
#                 "margin": "lg"
#             },
#             {
#                 "type": "text",
#                 "text": "ระบบทำงานเสร็จสมบูรณ์",
#                 "margin": "lg",
#                 "size": "sm",
#                 "color": "#00a0e9",
#                 "align": "center"
#             }
#         ])

#         # สร้าง Flex Message
#         flex_message = FlexSendMessage(
#             alt_text="สรุปผลการทำงาน DNC",
#             contents=contents
#         )

#         # ส่งข้อความ
#         line_bot_api_user = LineBotApi(line_access_token_user)
#         line_bot_api_user.push_message(user_id, flex_message)
        
#         logging.info("Flex notification sent successfully")
#         return True
        
#     except LineBotApiError as e:
#         logging.error(f"Line API error: {e}")
#         return False
#     except Exception as e:
#         logging.error(f"Error sending Flex notification: {e}")
#         return False
    
# # Connect DB
# def ConOracle():
#     try:
        
#         env = os.getenv('ENV', 'dev')
#         db_host = config.get(env, 'host')
#         db_port = config.get(env, 'port')
#         db_username = config.get(env, 'username')
#         db_password = config.get(env, 'password')
#         db_name = config.get(env, 'dbname')
        
#         dsn_name = oracledb.makedsn(db_host, db_port, service_name=db_name)
#         conn = oracledb.connect(user=db_username, password=db_password, dsn=dsn_name)

#         cursor = conn.cursor()
#         print(f"Connecting database {db_name}")
#         return cursor, conn
#     except oracledb.Error as error:
#         message = f"เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB : {error}"
#         send_flex_notification_start(message)
#         print("เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB:", error)
#         return message, None
    
# # def ConOracle():
# #     try:
# #         env = os.getenv('ENV', 'dev')
# #         db_host = config.get(env, 'host')
# #         db_port = config.get(env, 'port')
# #         db_username = config.get(env, 'username')
# #         db_password = config.get(env, 'password')
# #         db_name = config.get(env, 'dbname')

# #         # สร้าง DSN สำหรับ cx_Oracle
# #         dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_name)
        
# #         # สร้าง connection ด้วย cx_Oracle
# #         conn = cx_Oracle.connect(
# #             user=db_username,
# #             password=db_password,
# #             dsn=dsn
# #         )
        
# #         cursor = conn.cursor()
# #         print(f"Connecting database {db_name} using cx_Oracle")
# #         return cursor, conn
        
# #     except cx_Oracle.Error as error:
# #         message = f"เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB : {error}"
# #         send_flex_notification_start(message)
# #         print("เกิดข้อผิดพลาดในการเชื่อมต่อกับ Oracle DB:", error)
# #         return message, None
    
        
# # Default arguments
# default_args = {
#     "owner": "DNC airflow",
#     "depends_on_past": False,
#     "retries":3,
#     "retry_delay":timedelta(seconds=10)
# }

# # DAG definition
# with DAG(
#     dag_id="DNC",
#     default_args=default_args,
#     catchup=False,
#     description="DNC airflow",
#     # tags=["Dev"],
#     start_date=datetime(2024, 4, 24, 16, 30, 0, 0, tzinfo=local_tz),
#     schedule_interval="40 20 * * 1-7",
# ) as dag:
    
#     @task
#     def Get_dnc_work():
        
#         send_flex_notification_start()
        
#         cursor, conn = ConOracle()
        
#         try:
#             query = f"""
#                       SELECT
#                             q.*, 
#                             l.leadname, 
#                             l.leadsurname,
#                             a.assignstatus,
#                             -- QC ชื่อ
#                             (SELECT bytedes 
#                             FROM tqmsale.sysbytedes 
#                             WHERE tablename = 'LEADQC' 
#                             AND columnname = 'QCCODE' 
#                             AND bytecode = q.qccode
#                             FETCH FIRST 1 ROWS ONLY) AS QCNAME,

#                             -- ตรวจสอบสถานะ H (แสดง 'Y' ถ้ามีสถานะ H, 'N' ถ้าไม่มี)
#                             CASE 
#                                 WHEN EXISTS (
#                                     SELECT 1 
#                                     FROM tqmsale.leadbypassrequest tx 
#                                     WHERE tx.leadid = a.leadid 
#                                     AND tx.leadassignid = a.leadassignid
#                                     AND tx.bypassstatus = 'H'
#                             ) THEN 'Y'
#                             ELSE 'N'
#                             END AS has_bypass_h,
#                             tx.DNCENDDATE
#                             FROM 
#                                 tqmsale.leadqc q
#                                 LEFT JOIN tqmsale.leadassign a ON q.leadid = a.leadid AND q.leadassignid = a.leadassignid
#                                 JOIN tqmsale.VIEW_LEAD_LO l ON q.leadid = l.leadid
#                                 LEFT JOIN tqmsale.leadbypassrequest tx ON q.leadid = tx.leadid AND q.leadassignid = tx.leadassignid
#                             WHERE 1=1
#                             --AND q.QCCODE = '10'
#                             AND q.qcstatus = 'S'
#                             AND tx.BYPASSSTATUS = 'H'
#                             {dnc_dates}
#                           """
            
#             print("Fetching data...")
#             cursor.execute(query)
#             df = pd.DataFrame(
#                 cursor.fetchall(), columns=[desc[0] for desc in cursor.description]
#             )
            
#             # print(query)
#             # df = pd.read_sql(sql, conn)
                
#             formatted_table = df.to_markdown(index=False)
#             print(f"\n{formatted_table}")
#             print(f"Get data successfully")
#             print(f"df: {len(df)}")
            
#             message = f"\nข้อมูล DNC มีทั้งหมดรวม {len(df)} รายการ"
#             print(message)
#             # send_flex_notification_end(message)
            
#             return {"Get_dnc_work":df}
#         except oracledb.Error as error:
#             message = f'เกิดข้อผิดพลาดในการเรียกงาน DNC : {error}'
#             # message = f"Fail with task {task_id} \n error : {error}"
#             send_flex_notification_start(message)
#         finally:
#             cursor.close()
#             conn.close() 
    
#     @task
#     def update_leadbypassrequest_status(**kwargs):
#         ti = kwargs["ti"]
#         task_id = kwargs['task_instance'].task_id
#         try_number = kwargs['task_instance'].try_number
#         message = f"Processing task {task_id} ,try_number {try_number}"
#         # print(f"{message}")
#         # send_flex_notification_start(message)
        
#         result = ti.xcom_pull(task_ids="Process_x.Get_dnc_work", key="return_value")
        
#         # แสดงผลลัพธ์ที่ดึงมาจาก XCom
#         print("XCom result from Get_dnc_work:", result)
        
#         df = result["Get_dnc_work"]
        
#         cursor, conn = ConOracle()
        
#         try:
#             if df is None or df.empty:
#                 print("DataFrame is empty. Exiting task.")
#                 return {"Update_x_sum": df}
#             else:
#                 update_status_query_X = """
#                         UPDATE TQMSALE.LEADBYPASSREQUEST Q
#                         SET Q.BYPASSSTATUS = 'X'
#                         WHERE Q.LEADID = :leadid
#                         AND Q.LEADASSIGNID = :leadassignid
#                         AND Q.BYPASSSTATUS = 'H'
#                     """
                    
#                 i = 0
#                 for index, row in df.iterrows():
#                     cursor.execute(update_status_query_X, {'leadid': row['LEADID'], 'leadassignid': row['LEADASSIGNID']})
#                     print(f"Number: {i+1} Updated Bypassstatus to X row {index}: leadid={row['LEADID']}, leadassignid={row['LEADASSIGNID']}")
#                     i+=1
                    
#                 update_status_query_N = """
#                         UPDATE TQMSALE.LEADASSIGN a
#                         SET a.ASSIGNSTATUS = 'N'
#                         WHERE a.LEADID = :leadid
#                         AND a.LEADASSIGNID = :leadassignid
#                         AND a.ASSIGNSTATUS NOT IN ('N')
#                     """
                    
#                 i = 0
#                 for index, row in df.iterrows():
#                     cursor.execute(update_status_query_N, {'leadid': row['LEADID'], 'leadassignid': row['LEADASSIGNID']})
#                     print(f"Number: {i+1} Updated Assignstatus to N row {index}: leadid={row['LEADID']}, leadassignid={row['LEADASSIGNID']}")
#                     i+=1
                
#                 conn.commit() 
#                 print("All updates committed successfully.")
#                 formatted_table = df.to_markdown(index=False)
#                 print(f"\n{formatted_table}")
#                 message = f"จำนวนรายการที่ปรับสถานะรวม {df} รายการ"
#                 print(message)
#                 return {"Update_x_sum": df}
        
#         except oracledb.Error as error:
#             message = f"Fail with task {task_id} \n error : {error}"
#             send_flex_notification_start(message)
#             conn.rollback()
#             return None
        
#         finally:
#             if cursor:
#                 cursor.close()
#             if conn:
#                 conn.close()        
                
#     @task
#     def Split_qccode_dnc(**kwargs):
#         ti = kwargs["ti"]
#         task_id = kwargs['task_instance'].task_id
#         try_number = kwargs['task_instance'].try_number
#         message = f"Processing task {task_id} ,try_number {try_number}"
#         print(f"{message}")
        
#         result = ti.xcom_pull(task_ids="Process_x.update_leadbypassrequest_status", key="return_value")
#         df = result.get("Update_x_sum", pd.DataFrame())
        
#         try:
#             df_DNC = df.query("QCCODE == '10'")
#             df_V2T = df.query("QCCODE == '11'")
#             df_AI = df.query("QCCODE == '12'")
#             df_MISMATCH_NUM = df.query("QCCODE == '21'")
        
#             print("QCCODE 'Do not call list' (10):\n", df_DNC)
#             print("QCCODE 'V2T' (11):\n", df_V2T)
#             print("QCCODE 'AI-Do not call' (12):\n", df_AI)
#             print("QCCODE 'Mismatch number' (21):\n", df_MISMATCH_NUM)
            
#             print (f'{"SUM จำนวนงานทั้งหมดที่ปรับสถานะ",(len(df_DNC) + len(df_V2T) + len(df_AI) + len(df_MISMATCH_NUM))}')


#             # print("Merged DataFrame (df_actioncode):\n", df_actioncode)

#         except Exception as e:
#             message = f"Fail with task {task_id} \n error : {e}"
#             send_flex_notification_start(message)
#             print(f"Split_actioncode_ac Error: {e}")
#             return {}

#         return {
#             "df_DNC": df_DNC,
#             "df_V2T": df_V2T,
#             "df_AI": df_AI,
#             "df_MISMATCH_NUM": df_MISMATCH_NUM
#         }
        
#     @task
#     def notify_final_result(**kwargs):
#         ti = kwargs["ti"]
#         task_id = kwargs['task_instance'].task_id
#         try_number = kwargs['task_instance'].try_number
#         message = f"Processing task {task_id} ,try_number {try_number}"
#         print(f"{message}")
        
#         # ดึงผลลัพธ์จาก task ก่อนหน้า
#         qccode_results = ti.xcom_pull(task_ids="Process_x.Split_qccode_dnc", key="return_value")
        
#         # ส่งการแจ้งเตือนแบบ Flex Message
#         send_flex_notification_end(qccode_results)
        
#         return {"status": "success"}

#     start = EmptyOperator(task_id="start", trigger_rule="none_failed_min_one_success")
#     end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    
#     @task_group
#     def Process_x():
#         Get_dnc_task = Get_dnc_work()
#         Update_x_task = update_leadbypassrequest_status()
#         Split_qccode_task = Split_qccode_dnc()
#         Notify_final_task = notify_final_result()
        
#         Get_dnc_task >> Update_x_task >> Split_qccode_task >> Notify_final_task
        
#     Process_x_group = Process_x()
    
#     (
#     start >> Process_x_group >> end
#     )