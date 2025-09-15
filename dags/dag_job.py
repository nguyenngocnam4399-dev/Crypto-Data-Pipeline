from airflow import DAG
from airflow.operators.python import PythonOperator
#setup: pip install mysql-connector-python
import mysql.connector # type: ignore
#setup: pip install requests
import requests # type: ignore
from datetime import datetime
import time
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Kết nối MySQL
def connect_mysql():
    return mysql.connector.connect(
        host="mysql",      # địa chỉ server MySQL
        port = 3306,
        user="root",           # user MySQL
        password="1234",   # mật khẩu MySQL
        database="thesis"   # database của bạn
    )

def test_connection():
    with connect_mysql() as conn:   # tự đóng kết nối
        print("✅ Kết nối MySQL thành công!")


# trích xuất symbol_name, interval_name
def get_list_from_db(query, key):
    with connect_mysql() as conn, conn.cursor(dictionary=True) as cursor:
        cursor.execute(query)
        return [row[key] for row in cursor.fetchall()]

def get_all_metadata(**context):
    symbols = get_list_from_db("SELECT symbol_name FROM symbol_dim", "symbol_name")
    intervals = get_list_from_db("SELECT interval_name FROM interval_dim", "interval_name")
    context['ti'].xcom_push(key="symbols", value=symbols)
    context['ti'].xcom_push(key="intervals", value=intervals)


# Hàm lấy hoặc tạo symbol_id, interval_id
def get_or_create_id(conn, cursor, table, id_col, name_col, name_value):
    cursor.execute(f"SELECT {id_col} FROM {table} WHERE {name_col}=%s", (name_value,))
    result = cursor.fetchone()
    if result:
        return result[id_col]
    cursor.execute(f"INSERT INTO {table} ({name_col}) VALUES (%s)", (name_value,))
    conn.commit()
    return cursor.lastrowid


# Binance API
def fetch_klines(symbol, interval, limit, start_time=None, max_retry=10):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time:
        params["startTime"] = start_time  # chỉ lấy từ thời điểm mới nhất trong DB

    for attempt in range(1, max_retry + 1):
        try:
            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()  # báo lỗi nếu HTTP != 200
            return res.json()
        except Exception as e:
            print(f"[{symbol}-{interval}] Lỗi lần {attempt}: {e}")
            if attempt == max_retry:
                raise  # hết retry thì quăng lỗi ra ngoài
            print("⏳ Đợi 10s rồi thử lại...")
            time.sleep(10)

def insert_kline(conn, cursor, symbol, interval, data):
    symbol_id = get_or_create_id(conn, cursor, "symbol_dim", "symbol_id", "symbol_name", symbol)
    interval_id = get_or_create_id(conn, cursor, "interval_dim", "interval_id", "interval_name", interval)

    for kline in data:
        open_time = datetime.fromtimestamp(kline[0] / 1000)
        open_price = kline[1]
        high_price = kline[2]
        low_price = kline[3]
        close_price = kline[4]
        volume = kline[5]
        close_time = datetime.fromtimestamp(kline[6] / 1000)
        
        cursor.execute("""
            INSERT IGNORE INTO kline_fact 
            (symbol_id, interval_id, open_time, open_price, high_price, low_price, close_price, volume, close_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (symbol_id, interval_id, open_time, open_price, high_price, low_price, close_price, volume, close_time))
    conn.commit()


def fetch_and_insert (**context):
    ti = context['ti']
    symbols = ti.xcom_pull(key='symbols', task_ids='Get_metadata')
    intervals = ti.xcom_pull(key='intervals', task_ids='Get_metadata')
    
    with connect_mysql() as conn, conn.cursor(dictionary=True) as cursor:
        for symbol in symbols:
            for interval in intervals:
                # Lấy max open_time hiện có
                cursor.execute("""
                    SELECT MAX(open_time) AS last_time
                    FROM kline_fact k
                    JOIN symbol_dim s ON k.symbol_id = s.symbol_id
                    JOIN interval_dim i ON k.interval_id = i.interval_id
                    WHERE s.symbol_name=%s AND i.interval_name=%s
                """, (symbol, interval))
                row = cursor.fetchone()
                last_time = row["last_time"]

                start_time = int(last_time.timestamp() * 1000) if last_time else None # Binance cần ms
                try:
                    klines = fetch_klines(symbol, interval, limit=1000, start_time=start_time)
                    if klines:
                        insert_kline(conn, cursor, symbol, interval, klines)
                        print(f"💾 Insert thành công {len(klines)} rows cho {symbol}-{interval}")
                    else:
                        print(f"⚠️ Không có dữ liệu cho {symbol}-{interval}")
                except Exception as e:
                    print(f"[{symbol} - {interval}] Lỗi: {e}")


# Airflow DAG
with DAG(
    dag_id = "Thesis",
    start_date = datetime(2025, 9, 9),
    schedule = '@daily',
    catchup = False
) as dag:

    # Task 1: Connect MySQL
    Connecting_mysql = PythonOperator(
        task_id="Connecting_mysql",
        python_callable=test_connection
    )
    
    # Task 2: Get all symblos and intervals
    Get_metadata = PythonOperator(
        task_id="Get_metadata",
        python_callable=get_all_metadata
    )

    # Task 3: Insert Data
    Fetching_and_inserting = PythonOperator(
        task_id = "Fetching_and_inserting",
        python_callable = fetch_and_insert
    )

    compute_indicators = SparkSubmitOperator(
        task_id="Compute_indicators",
        application="/opt/airflow/dags/spark_job.py",
        conn_id="spark_default",
        name="arrow-spark",
        conf={"spark.master": "local[*]"},
        deploy_mode="client",
        packages="mysql:mysql-connector-java:8.0.33",
        dag=dag,
    )

Connecting_mysql >> Get_metadata >> Fetching_and_inserting >> compute_indicators