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
            SELECT * FROM TQMSALE.PROSPECT
            """
            df = pd.read_sql(sql, conn)
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