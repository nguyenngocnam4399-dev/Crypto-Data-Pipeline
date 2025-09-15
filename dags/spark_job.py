from pyspark.sql import SparkSession, functions as F, Window
import mysql.connector

DB_CONFIG = {
    "host": "mysql",
    "port": "3306",
    "user": "root",
    "password": "1234",
    "database": "thesis"
}


def compute_indicators():
    spark = SparkSession.builder.appName("IndicatorsJob").config("spark.sql.shuffle.partitions", "8").getOrCreate()


    # Đọc dữ liệu mới nhất từ MySQL
    df = spark.read.format("jdbc").options(
        url=f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="kline_fact",
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    ).load().select("symbol_id", "interval_id", "close_time", "close_price").cache()


    w = Window.partitionBy("symbol_id", "interval_id").orderBy("close_time")


    sma_df = df.withColumn("value", F.avg("close_price").over(w.rowsBetween(-13, 0))).withColumn("type", F.lit("SMA"))


    tmp = df.withColumn("diff", F.col("close_price") - F.lag("close_price").over(w)) \
        .withColumn("gain", F.when(F.col("diff") > 0, F.col("diff")).otherwise(0.0)) \
        .withColumn("loss", F.when(F.col("diff") < 0, -F.col("diff")).otherwise(0.0))
    avg_gain = F.avg("gain").over(w.rowsBetween(-13, 0))
    avg_loss = F.avg("loss").over(w.rowsBetween(-13, 0))
    rsi_df = tmp.withColumn("rs", avg_gain / avg_loss) \
        .withColumn("value", 100 - (100 / (1 + F.col("rs")))) \
        .withColumn("type", F.lit("RSI"))


    bb_df = df.withColumn("mean", F.avg("close_price").over(w.rowsBetween(-13, 0))) \
        .withColumn("stddev", F.stddev("close_price").over(w.rowsBetween(-13, 0)))
    bb_up_df = bb_df.withColumn("value", F.col("mean") + 2 * F.col("stddev")).withColumn("type", F.lit("BB_UP"))
    bb_down_df = bb_df.withColumn("value", F.col("mean") - 2 * F.col("stddev")).withColumn("type", F.lit("BB_DOWN"))


    select_cols = ["symbol_id", "interval_id", "type", "value", F.col("close_time").alias("timestamp")]


    indicators_df = (sma_df.select(*select_cols)
        .unionByName(rsi_df.select(*select_cols))
        .unionByName(bb_up_df.select(*select_cols))
        .unionByName(bb_down_df.select(*select_cols))
        .filter(F.col("value").isNotNull()))

    # Lọc record đã tồn tại bằng left_anti join
    existing_df = spark.read.format("jdbc").options(
        url=f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="indicator_fact",
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    ).load().select("symbol_id", "interval_id", "type", "timestamp")


    indicators_df = indicators_df.join(existing_df, on=["symbol_id", "interval_id", "type", "timestamp"], how="left_anti")


    if not indicators_df.rdd.isEmpty():
        indicators_df.write.format("jdbc").options(
            url=f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            driver="com.mysql.cj.jdbc.Driver",
            dbtable="indicator_fact",
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        ).mode("append").save()
        print("[DONE] Written new indicators.")
    else:
        print("[DONE] No new indicators.")
    spark.stop()


if __name__ == "__main__":
    compute_indicators()    