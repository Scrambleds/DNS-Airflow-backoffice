import pandas as pd
import oracledb
import configparser
from datetime import datetime
import os

class DataProcessor:
    def __init__(self, config_path=None):
        self.conn_info = self._load_config(config_path)

    def _load_config(self, config_path):
        config_path = config_path or os.path.join(os.path.dirname(__file__), '../config/db_config.cfg')
        config = configparser.ConfigParser()
        config.read(config_path)
        oracle = config['dev']
        return {
            'user': oracle.get('user'),
            'password': oracle.get('password'),
            'dsn': f"{oracle.get('host')}:{oracle.get('port')}/{oracle.get('service')}"
        }

    def query_data(self):
        with oracledb.connect(**self.conn_info) as conn:
            sql = """
             SELECT 
                    q.*, 
                    l.leadname, 
                    l.leadsurname,
                    a.assignstatus,
                    
                    -- ทะเบียนรถ
                    (SELECT plateid 
                    FROM tqmsale.leadcar c 
                    WHERE c.leadid = a.leadid 
                    AND c.leadcarid = a.leadcarid
                    FETCH FIRST 1 ROWS ONLY) AS PLATEID,
                    
                    -- ข้อมูล Staff
                    (SELECT d.departmentcode || ':' || f.staffcode || ':' || f.staffname  
                    FROM tqmsale.V_STAFF f 
                    JOIN tqmsale.department d ON f.departmentid = d.departmentid 
                    WHERE f.staffid = a.staffid
                    FETCH FIRST 1 ROWS ONLY) AS STAFF,
                    
                    -- QC ชื่อ
                    (SELECT bytedes 
                    FROM tqmsale.sysbytedes 
                    WHERE tablename = 'LEADQC' 
                    AND columnname = 'QCCODE' 
                    AND bytecode = q.qccode
                    FETCH FIRST 1 ROWS ONLY) AS QCNAME,

                    -- แจ้งห้ามโทร
                    (SELECT MAX('<br><font color=red><b>แจ้งห้ามโทร::' || TO_CHAR(requestdatetime,'YY/MM/DD') || '</b><br><font color=#992211>' || remark)
                    FROM tqmsale.leadbypassrequest tx 
                    WHERE tx.leadid = a.leadid 
                    AND tx.leadassignid = a.leadassignid) AS BYPASSREQUEST,
                    
                    -- ผลลัพธ์การติดต่อล่าสุด
                    (SELECT resultcode || ':' || resultname 
                    FROM tqmsale.result 
                    WHERE resultid = a.resultid
                    FETCH FIRST 1 ROWS ONLY) AS RESULT
                FROM 
                    tqmsale.leadqc q
                    LEFT JOIN tqmsale.leadassign a ON q.leadid = a.leadid AND q.leadassignid = a.leadassignid
                    JOIN tqmsale.VIEW_LEAD_LO l ON q.leadid = l.leadid
                WHERE 1=1
                --AND q.actionstaffid = :staffid
                    -- เงื่อนไขเพิ่มเติมตามสถานะ
                AND q.QCCODE = 10
                AND q.qcstatus = 'W'
            """
            df = pd.read_sql(sql, conn)
            
            formatted_table = df.to_markdown(index=False)
            print(formatted_table)
            
        return df

    def filter_today_block(self, df):
        today = datetime.now().date()
        filtered_df = df[df['block_date'] == str(today)]
        return filtered_df

    def update_status(self, df):
        if df.empty:
            print("No data to update.")
            return
        with oracledb.connect(**self.conn_info) as conn:
            cursor = conn.cursor()
            ids_to_update = df['id'].tolist()
            cursor.executemany(
                "UPDATE your_table SET status = 'X' WHERE id = :1",
                [(i,) for i in ids_to_update]
            )
        # conn.commit()
        print(f"Updated {len(ids_to_update)} rows.")