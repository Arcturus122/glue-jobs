import yfinance as yf
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StructField, StructType, StringType, DateType, FloatType


tickers = {
    "Brent": "BZ=F",
    "WTI": "CL=F",
    "Gold": "GC=F",
    "Silver": "SI=F",
    "Copper": "HG=F",
    "Henry Hub": "NG=F",
    "TTF": "TTF=F",
    "Corn": "ZC=F",
    "Soybeans": "ZS=F",
    "Wheat": "ZW=F",
    "Cotton": "CT=F",
    "Sugar": "SB=F",
    "Coffee": "KC=F",
    "Cocoa": "CC=F",
    "Orange Juice": "OJ=F",
    "Lumber": "LBS=F",
    "Live Cattle": "LE=F",
    "Feeder Cattle": "GF=F",
    "Lean Hogs": "HE=F",
    "Zinc": "ZN=F",
    "Aluminum": "ALI=F",
}

if __name__ == "__main__":
    sc = SparkContext()
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session
    connection_postgresql_options = {
        "url": "jdbc:postgresql://ladubief-postgre-db-1.cywjasf2p0qq.us-east-1.rds.amazonaws.com:5432/ladubief",
        "dbtable": "integration.dummy_data",
        "user": "ladubief",
        "password": "Dubief-74600",
    }

    # fetching data and creating dataframe
    logger.info("Fetching data from Yahoo Finance")
    data = yf.download(list(tickers.values()), period="max", interval="1d")
    data = (
        data.unstack(level=0)
        .to_frame()
        .reset_index()
        .rename(
            columns={
                "level_0": "metric",
                0: "value",
                "level_1": "ticker",
                "Date": "date",
            }
        )
        .set_index(["date", "ticker", "metric"])
        .sort_index()
        .reset_index()
    )
    data.dropna(inplace=True)
    data = (
        data.pivot_table(index=["date", "ticker"], columns="metric", values="value")
        .reset_index()
        .drop(columns=["Adj Close"])
    )
    data.columns = data.columns.str.lower()
    data.insert(1, "commodity", data["ticker"].map({v: k for k, v in tickers.items()}))
    data = data.loc[
        :, ["date", "commodity", "ticker", "open", "high", "low", "close", "volume"]
    ]
    col_spark = StructType(
        [
            StructField("date", DateType(), True),
            StructField("commodity", StringType(), True),
            StructField("ticker", StringType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", FloatType(), True),
        ]
    )
    spark_data = spark.createDataFrame(data, schema=col_spark)
    glue_df = DynamicFrame.fromDF(spark_data, glueContext, "yahoo_finance")

    # writing data to postgresql
    logger.info("Writing data to PostgreSQL")
    connection_postgresql_options["dbtable"] = "integration.yahoo_finance"
    glueContext.write_dynamic_frame_from_options(
        frame=glue_df,
        connection_type="postgresql",
        connection_options=connection_postgresql_options,
    )
    logger.info("Job finished")
