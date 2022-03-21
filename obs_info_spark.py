import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField
from pyspark.sql.functions import regexp_extract, lit, split, col

from datetime import datetime
import requests
import json
import os
import configparser
from pathlib import Path

findspark.init()

spark = SparkSession \
        .builder \
        .master('local') \
        .appName('tempera_obs') \
        .config("spark.driver.extraClassPath", "/Users/mdgome/Downloads/mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar") \
        .getOrCreate()

now_datetime=datetime.now().strftime('%Y.%m.%d')

config = configparser.ConfigParser()
config.read(Path("./config/config.ini"),encoding='utf-8')

service_key=config['api']['service_key']
api_url = config['api']['url'].format(service_key=service_key)
response = requests.get(api_url).json()

df = spark.createDataFrame(response["result"]["data"])

config = configparser.ConfigParser()
config.read(Path("./config/config.ini"),encoding='utf-8')
# config.read(os.getcwd()+os.sep+'config'+os.sep+'config.ini',encoding='utf-8')

user = config['dev_mysql']['user']
password = config['dev_mysql']['password']
host = config['dev_mysql']['host']
port = config['dev_mysql']['port']
dbname = config['dev_mysql']['dbname']
url = config['dev_mysql']['url'].format(host=host,port=port,dbname=dbname)

df.write.format('jdbc').options(
    url=url,
    driver='com.mysql.jdbc.Driver',
    dbtable='obs_info',
    user=user,
    password=password).mode('overwrite').save()

file_name = "hdfs:///middle_project/"+now_datetime+"_obs_info.csv"
df.coalesce(1).write.format('com.databricks.spark.csv').save(file_name,header = 'true')

spark.stop()