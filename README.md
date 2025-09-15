# 📈 Crypto Data Pipeline

## 📑 Table of Contents
1. [Introduction](#introduction)  
2. [Features](#features)  
3. [Technologies Used](#technologies-used)  
4. [Install (Docker)](#install-docker)  
5. [Usage](#usage)  
6. [Examples](#examples)  
7. [Acknowledgments](#acknowledgments)  

---

## 🚀 Introduction
This project builds a data pipeline for cryptocurrency processing:  
- Crawl news from **Coindesk** and **NewsBTC**.  
- Store data in **MySQL** (news, tag, kline, indicators).  
- Use **Airflow DAGs** for orchestration:  
  - `crawl.py`: Crawl news + sentiment analysis.  
  - `dag_job.py`: Fetch kline data from Binance and load into MySQL.  
  - `spark_job.py`: Compute technical indicators (**SMA, RSI, Bollinger Bands**).  

---

## 🚀 Features
- Scheduled news crawling (Airflow DAG `Crawl_crypto_news`).  
- Store news data in `news_fact`, `dim_tag` tables.  
- Crawl **kline** data from Binance API → store in `kline_fact`.  
- Compute technical indicators (**SMA, RSI, BB_UP, BB_DOWN**) and store in `indicator_fact`.  
- Manage workflow with **Airflow**.  

---

## 🛠 Technologies Used
- **Python 3.9+**  
- **Apache Airflow** (Docker)  
- **Apache Spark** (Docker)  
- **MySQL 8** (Docker)  
- **Requests, BeautifulSoup4** (data crawling)  
- **NLTK Vader** (sentiment analysis)  
- **Binance API**  

---

## ⚙️ Install (Docker)
1. Clone the project:  
   ```bash
   git clone https://github.com/your-username/crypto-pipeline.git
   cd crypto-pipeline
   ```

2. Start Docker Compose:  
   ```bash
   docker-compose up -d --build
   ```

   - Containers will include:  
     - `mysql` (service running MySQL, containing database `thesis`)  
     - `airflow` (webserver, scheduler)  
     - `spark` (running Spark job)  

3. Initialize the database:  
   - Mount the `sql/` directory into the MySQL container.  
   - MySQL will automatically execute the scripts:  
     - `sql/kline_dim_fact.sql`  
     - `sql/indicator_fact.sql`  
     - `sql/crawl.sql`  

4. Access the Airflow UI at:  
   ```
   http://localhost:8080
   ```
   (default user/pass: `airflow/airflow` if not changed in `docker-compose.yaml`).  

---

## ▶️ Usage
- **Run news crawling DAG**: `Crawl_crypto_news`.  
- **Run Binance + Spark job DAG**: `Thesis`.  
- Airflow will automatically:  
  1. Crawl news → save to MySQL.  
  2. Fetch kline data from Binance.  
  3. Compute technical indicators with Spark → store in MySQL.  

---

## 📊 Examples
### Example news data (`news_fact`):
| id | title | url | sentiment_score | created_date | tag_name |  
|----|-------|-----|----------------|--------------|----------|  
| 1 | Bitcoin Price Surges | https://www.coindesk.com/... | 0.67 | 2025-09-14 12:00:00 | Bitcoin |  

### Example indicator data (`indicator_fact`):
| id | symbol_id | interval_id | type | value | timestamp |  
|----|-----------|-------------|------|-------|------------|  
| 1 | 1 | 2 | SMA | 42000.123 | 2025-09-14 12:00:00 |  
| 2 | 1 | 2 | RSI | 55.67 | 2025-09-14 12:00:00 |  

---

## 🙏 Acknowledgments
- [Binance API](https://binance-docs.github.io/apidocs/)  
- [NLTK Vader Sentiment](https://www.nltk.org/_modules/nltk/sentiment/vader.html)  
- [Apache Airflow](https://airflow.apache.org/)  
- [Apache Spark](https://spark.apache.org/)  
