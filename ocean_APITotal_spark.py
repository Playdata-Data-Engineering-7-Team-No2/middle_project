import findspark
findspark.init()

### PySpark 관련 패키지 import
from pyspark.sql import SparkSession
# from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField
from pyspark.sql.types import *
from pyspark.sql.functions import *
### api 처리 패키지 import
from datetime import datetime, timedelta
import requests
import json
### General 패키지 import
import configparser
from pathlib import Path

### Local SparkCluster connected session
spark = SparkSession \
        .builder \
        .master('spark://hadoop01:7077') \
        .config("spark.driver.extraClassPath", "/hadoop/jupyter_dir/jar/mysql-connector-java-8.0.28/mysql-connector-java-8.0.28.jar") \
        .config("spark.driver.extraClassPath", "hdfs:///spark3-jars/mysql-connector-java-8.0.28.jar") \
        .appName('tempera_obs') \
        .getOrCreate()

#### DB Server Configure & API Service Key
config = configparser.ConfigParser()
config.read(Path("./config/config.ini"),encoding='utf-8')
# config.read(os.getcwd()+os.sep+'config'+os.sep+'config.ini',encoding='utf-8')

user = config['dev_mysql']['user']
password = config['dev_mysql']['password']
host = config['dev_mysql']['host']
port = config['dev_mysql']['port']
dbname = config['dev_mysql']['dbname']
url = config['dev_mysql']['url'].format(host=host,port=port,dbname=dbname)
service_key=config['api']['service_key']
# dbtable = config['dev_mysql']['dbtable']

#### 관측소 정보 table 불러옴
# obs_post_data = pd.read_csv("./관측소 정보.csv")
obs_post_data= spark.read.format('jdbc').options(
    url=url,
    driver='com.mysql.jdbc.Driver',
    dbtable='obs_info',
    user=user,
    password=password).load()

#### 전날 데이터 불러오기 위한 관련 코드
yesterday = datetime.today() - timedelta(days = 1 )
yesterday = yesterday.strftime('%Y%m%d')

# 기온 api
url="http://www.khoa.go.kr/api/oceangrid/tideObsAirTemp/search.do?ServiceKey="+service_key+"&ObsCode=DT_0004&Date="+yesterday+"&ResultType=json"
# 기압 api
url1="http://www.khoa.go.kr/api/oceangrid/tideObsAirPres/search.do?ServiceKey="+service_key+"&ObsCode=DT_0004&Date="+yesterday+"&ResultType=json"
# 풍속 api
url2="http://www.khoa.go.kr/api/oceangrid/tideObsWind/search.do?ServiceKey="+service_key+"&ObsCode=DT_0004&Date="+yesterday+"&ResultType=json"
# 염분 api
url3="http://www.khoa.go.kr/api/oceangrid/tideObsSalt/search.do?ServiceKey="+service_key+"&ObsCode=DT_0004&Date="+yesterday+"&ResultType=json"
# 조위 실측/예측 api
url4="http://www.khoa.go.kr/api/oceangrid/tideCurPre/search.do?ServiceKey="+service_key+"&ObsCode=DT_0004&Date="+yesterday+"&ResultType=json"
# 수온 api
url5="http://www.khoa.go.kr/api/oceangrid/tideObsTemp/search.do?ServiceKey="+service_key+"&ObsCode=DT_0004&Date="+yesterday+"&ResultType=json"

#### API data 가져옴
response = requests.get(url).json()
response1 = requests.get(url1).json()
response2 = requests.get(url2).json()
response3 = requests.get(url3).json()
response4 = requests.get(url4).json()
response5 = requests.get(url5).json()

#### 가져온 data spark dataframe 으로 변환
df = spark.createDataFrame(response["result"]["data"])
df1 = spark.createDataFrame(response1["result"]["data"])
df2 = spark.createDataFrame(response2["result"]["data"])
df3 = spark.createDataFrame(response3["result"]["data"])
df4 = spark.createDataFrame(response4["result"]["data"])
df5 = spark.createDataFrame(response5["result"]["data"])

#### df1, df2, df3, df5 하나의 dataframe 으로 합침
output_df = df.join(df1,'record_time')
output_df = output_df.join(df2, 'record_time')
output_df = output_df.join(df3, 'record_time')
output_df = output_df.join(df5, 'record_time')

#### 모든 data type 이 string으로 되어 있어 data type 변경
output_df = output_df.withColumn('air_temp', output_df['air_temp'].cast(DoubleType()))\
    .withColumn('air_pres', output_df['air_pres'].cast(DoubleType()))\
    .withColumn('wind_dir', output_df['wind_dir'].cast(DoubleType()))\
    .withColumn('record_time', output_df['record_time'].cast("timestamp"))\
    .withColumn('salinity', output_df['salinity'].cast(DoubleType()))
    # .withColumn('pre_value', df4['pre_value'].cast(IntegerType())) \
    # .withColumn('real_value', df4['real_value'].cast(IntegerType()))

df4 = df4.withColumn('pre_value', df4['pre_value'].cast(IntegerType())) \
    .withColumn('record_time', df4['record_time'].cast("timestamp"))\
    .withColumn('real_value', df4['real_value'].cast(IntegerType()))

### Dataframe sort & Merge
output_df = output_df.sort(asc("record_time"))
union_df = output_df.unionByName(df4, allowMissingColumns=True)
union_df = union_df.sort(asc("record_time"))

#### DB data insert

union_df.write.format('jdbc').options(
    url=url,
    driver='com.mysql.jdbc.Driver',
    dbtable='DT_0004',
    user=user,
    password=password).mode('append').save()

#### 미완성
def test_obs_info():
    ## 관측소 별 호출 가능한 API 정리
    obs_info_df = obs_post_data.select(obs_post_data.obs_post_id,obs_post_data.obs_post_name,obs_post_data.obs_lat,obs_post_data.obs_lon,split(col("obs_object"), ",").alias("obs_object_array"),obs_post_data.data_type).drop("obs_object")
    obs_info_df = obs_info_df.sort(col("obs_object_array"),col("obs_post_id"))

    for row in obs_info_df.rdd.collect():
        print(row['obs_post_id'], row['obs_object_array'], row['data_type'])