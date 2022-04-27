from airflow import DAG
from airflow.operators.python import PythonOperator
import configparser
import requests
import pymysql
from glob import glob
from datetime import datetime, timedelta


from config.logger import get_logger

logger = get_logger("__openweathermap_dag__")
config = configparser.ConfigParser()
config.read("./config/config.ini")
# DB CONNECTION 추가..
# db_conn = ..
# db_cursor = db.conn.cursors()

try:
    conn_db = pymysql.connect(host='host', port=3306, user=config['DB']['USERNAME'], passwd=config['DB']['PASSWORD'], db='weather', charset='utf8')
    cursor = conn_db.cursor()
except:
    logging.error("DB Connection Issue")

        
'''
    ETL 구현 부분
'''
def extract(**context):

    location_names = [dir.replace("./data_lake/", "") for dir in glob("./data_lake/*")]
    # locations_name = context["location_names"]
    
    # DB로부터 location 정보(latitude, lontitude) 을 가져와야함
    # for location in 로케이션 정보: # 로케이션 정보에는 [[lat, lon], [lat2, lon2]] 이런 형태
    # 반복으로 데이터를 가져옴
    lat, lon = 37.541, 126.986
    api_url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={config['OPENWEATHERMAP']['API_KEY']}&units=metric&lang=kr" # 임시로 서울의 위도 경도로 탐색
    response = requests.get(api_url)
    
    if response.status_code == 200:
        logger.info("Request API Data Success")
    else:
        logger.info(f"{response.status_code}, {response.content}")
    
    task_instance = context["task_instace"]
    
    return (response.json())

def transform(**context):
    
    daily = context["task_instance"].xcom_pull(task_ids="extract")
    
    transform_results = []
    for i in range(1, 8):
        date_time = datetime.fromtimestamp(daily[i]["dt"]).strftime("%Y-%m-%d")
        sunrise = datetime.fromtimestamp(daily[i]["sunrise"]).strftime("%Y-%m-%d %H:%M:%S")
        sunset = datetime.fromtimestamp(daily[i]["sunset"]).strftime("%Y-%m-%d %H:%M:%S")
        moonrise = datetime.fromtimestamp(daily[i]["moonrise"]).strftime("%Y-%m-%d %H:%M:%S")
        moonset = datetime.fromtimestamp(daily[i]["moonset"]).strftime("%Y-%m-%d %H:%M:%S")
        morn_temp = f"{round(float(daily[i]['temp']['morn']), 1)}℃"
        eve_temp = f"{round(float(daily[i]['temp']['eve']), 1)}℃"
        min_temp = f"{round(float(daily[i]['temp']['min']), 1)}℃"
        max_temp = f"{round(float(daily[i]['temp']['max']), 1)}℃"
        morn_feels_like = f"{round(float(daily[i]['feels_like']['morn']), 1)}℃"
        eve_feels_like = f"{round(float(daily[i]['feels_like']['eve']), 1)}℃"
        humidity = f"{int(round(daily[i]['humidity'], 0))}%"
        pop = f"{int(round(daily[i]['pop'], 0))}%"
        weather_info = daily[i]["weather"][0]["main"]
        weather_detail_info = daily[i]["weather"][0]["description"]
        weather_icon = f"http://openweathermap.org/img/wn/{daily[i]['weather'][0]['icon']}@2x.png"
        transform_results.append({
            "datetime": date_time,
            "sunrise": sunrise,
            "sunset": sunset,
            "moonrise": moonrise,
            "moonset": moonset,
            "morn_temp": morn_temp,
            "eve_temp": eve_temp,
            "min_temp": min_temp,
            "max_temp": max_temp,
            "morn_feels_like": morn_feels_like,
            "eve_feels_like": eve_feels_like,
            "humidity": humidity,
            "pop": pop,
            "weather_info": weather_info,
            "weather_detail_info": weather_detail_info,
            "weather_icon": weather_icon
        })
    
    logger.info("transfrom data complete")
    
    return transform_results
    
def load(**context):
    
    location_names = [dir.replace("./data_lake/", "") for dir in glob("./data_lake/*")]
    data = context["task_instance"].xcom_pull(task_ids="transform")
    
    insert_query = f"""
                    INSERT INTO {table_name}
                    ('{column1}', '{column2}', '{column3}')
                    VALUES (%s, %s, %s)
                    """
    for index, row in data:
        db_cursor.execute(insert_query, data)
    
    logger.info("Load data Complete")
    
dag = DAG(
        dag_id = "openweather_daily",
        start_date = datetime(2022,4,25),
        schedule_interval = "0 0 * * *", 
        max_active_runs = 1,
        catchup = False,
        default_args = {
            "retries" : 1,
            "retry_delay" : timedelta(minutes=3)
        }
    )

extract = PythonOperator(
        task_id = "extract",
        python_callable = extract,
        params = {},
        provide_context = True,
        dag = dag
        )

transform = PythonOperator(
        task_id = "transform",
        python_callable = transform,
        params = {},
        provide_context = True,
        dag = dag
        )

load = PythonOperator(
        task_id = "load",
        python_callable = load,
        params = {},
        provide_context = True,
        dag = dag
        )


extract >> transform >> load