from pyspark.sql import SparkSession
from datetime import datetime
import json

def main():

    spark = SparkSession.builder.appName("ETL with PySpark").getOrCreate()

    path = "C:/Users/limseunghyun/Desktop/project/data_lake/남원읍/2022_2_21_18.json"

    df = spark.read.json(path)

    df.printSchema()

    print()
    
    # print(type(df))
# dataframe = spark.createDataFrame(data)
# dataframe.show()

if __name__ == "__main__":

    main()
