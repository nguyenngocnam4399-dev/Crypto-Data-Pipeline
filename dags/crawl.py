from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import mysql.connector
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re

# Download lexicon (chỉ chạy lần đầu)
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

# Regex kiểm tra link article hợp lệ trên Coindesk
COINDESK_REGEX = re.compile(r"^https:\/\/www\.coindesk\.com\/[a-z0-9-]+")
NEWSBTC_REGEX = re.compile(r"^https:\/\/www\.newsbtc\.com\/[a-z0-9-]+")

def is_valid_coindesk(url: str) -> bool:
    return COINDESK_REGEX.match(url) is not None

def is_valid_newsbtc(url: str) -> bool:
    return NEWSBTC_REGEX.match(url) is not None

# DB config
DB_CONFIG = {
    "host": "mysql",   # nếu chạy local thì đổi "localhost"
    "port": 3306,
    "user": "root",
    "password": "1234",
    "database": "thesis"
}

def get_db_conn():
    return mysql.connector.connect(**DB_CONFIG)

# ===== Crawl RSS từ Coindesk =====
def crawl_coindesk_rss(**context):
    rss_url = "https://www.coindesk.com/arc/outboundfeeds/rss/"
    resp = requests.get(rss_url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.text, "xml")

    articles = []
    for item in soup.find_all("item"):
        link = item.find("link").text.strip()
        if not is_valid_coindesk(link):
            continue

        title = item.find("title").text.strip()
        pub_date = item.find("pubDate").text.strip()
        created_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")

        category_el = item.find("category")
        tag = category_el.text.strip() if category_el else None

        articles.append({
            "title": title,
            "url": link,
            "created_date": created_date,
            "tag": tag
        })

    context["ti"].xcom_push(key="articles_coindesk", value=articles)

def crawl_newsbtc_rss(**context):
    rss_url = "https://www.newsbtc.com/feed/"
    resp = requests.get(rss_url, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(resp.text, "xml")

    articles = []
    for item in soup.find_all("item"):
        link = item.find("link").text.strip()
        if not is_valid_newsbtc(link):
            continue

        title = item.find("title").text.strip()
        pub_date = item.find("pubDate").text.strip()
        created_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
        category_el = item.find("category") or item.find("dc:creator")
        tag = category_el.text.strip() if category_el else None

        articles.append({
            "title": title,
            "url": link,
            "created_date": created_date,
            "tag": tag
        })

    context["ti"].xcom_push(key="articles_newsbtc", value=articles)


# ===== Crawl body + sentiment + insert MySQL =====
def process_articles(**context):
    articles = []
    articles.extend(context["ti"].xcom_pull(key="articles_coindesk", task_ids="crawl_coindesk_rss") or [])
    articles.extend(context["ti"].xcom_pull(key="articles_newsbtc", task_ids="crawl_newsbtc_rss") or [])
    if not articles:
        return

    conn = get_db_conn()
    cursor = conn.cursor()

    for art in articles:
        url = art["url"]

        # Check tồn tại
        cursor.execute("SELECT id FROM news_fact WHERE url=%s", (url,))
        if cursor.fetchone():
            continue

        try:
            # Crawl body
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(resp.text, "html.parser")
            paragraphs = [p.text.strip() for p in soup.find_all("p")]
            content = " ".join(paragraphs)
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

        # Sentiment
        score = sia.polarity_scores(content)["compound"]

        # Insert tag nếu có
        tag_id = None
        if art["tag"]:
            cursor.execute("INSERT IGNORE INTO dim_tag (tag_name) VALUES (%s)", (art["tag"],))
            conn.commit()
            cursor.execute("SELECT tag_id FROM dim_tag WHERE tag_name=%s", (art["tag"],))
            tag_id = cursor.fetchone()[0]

        # Insert vào news_fact
        cursor.execute("""
            INSERT INTO news_fact (title, url, sentiment_score, created_date, view_number, tag_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (art["title"], url, score, art["created_date"], None, tag_id))

    conn.commit()
    conn.close()

# ===== Airflow DAG =====
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="Crawl_crypto_news",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025, 9, 9),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="crawl_coindesk_rss",
        python_callable=crawl_coindesk_rss,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="crawl_newsbtc_rss",
        python_callable=crawl_newsbtc_rss,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="process_articles",
        python_callable=process_articles,
        provide_context=True,
    )

    [t1, t2] >> t3