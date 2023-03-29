import yfinance as yf
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

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
    # data = yf.download(list(tickers.values()), period="max", interval="1d")
    # data = (
    #     data.unstack(level=0)
    #     .to_frame()
    #     .reset_index()
    #     .rename(
    #         columns={
    #             "level_0": "metric",
    #             0: "value",
    #             "level_1": "ticker",
    #             "Date": "date",
    #         }
    #     )
    #     .set_index(["date", "ticker", "metric"])
    #     .sort_index()
    # )
    # data.dropna(inplace=True)
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    connection_postgresql_options = {
        "url": "jdbc:postgresql://ladubief-postgre-db-1.cywjasf2p0qq.us-east-1.rds.amazonaws.com:5432/ladubief",
        "dbtable": "integration.dummy_data",
        "user": "ladubief",
        "password": "Dubief-74600"
    }
    df = glueContext.create_dynamic_frame.from_options(connection_type="postgresql",
                                                          connection_options=connection_postgresql_options)
    
    print(df.head())
    