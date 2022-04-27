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
    conn_db = pymysql.connect(host='host', port=3306, user=config['dev_mysql']['USERNAME'], passwd=config['dev_mysql']['PASSWORD'], db='weather', charset='utf8')
    cursor = conn_db.cursor()
except:
    logging.error("DB Connection Issue")

    

'''
    ETL 구현 부분
'''
def extract(**context):

    location_names = [dir.replace("./data_lake/", "") for dir in glob("./data_lake/*")]
    location_names[0]
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
    
    data = context["task_instance"].xcom_pull(task_ids="extract")
    
    transform_results = []
    
    current_result = {
        "datetime": datetime.fromtimestamp(data["current"]["dt"]).strftime("%Y-%m-%d %H:%M:%S"),
        "temp": f"{round(float(data['current']['temp']), 1)}℃",
        "feels_like": f"{round(float(data['current']['feels_like']), 1)}℃",
        "humidity": f"{int(round(data['current']['humidity'], 0))}%",
        "uvi": data["current"]["uvi"],
        "wind": f"{data['current']['wind_deg']} {data['current']['wind_speed']}",
        "weather_info": data["current"]['weather'][0]["main"],
        "weather_detail_info": data["current"]['weather'][0]["description"],
        "weather_icon": f"http://openweathermap.org/img/wn/{data['current']['weather'][0]['icon']}@2x.png"
    }
    
    hourly_results = []
    for i in range(1, 24):
        date_time = datetime.fromtimestamp(data["hourly"][i]["dt"]).strftime("%Y-%m-%d %H:00:00")
        temp = f"{round(float(data['hourly'][i]['temp']), 1)}℃"
        feels_like = f"{round(float(data['hourly'][i]['feels_like']), 1)}℃"
        humidity = f"{int(round(data['hourly'][i]['humidity'], 0))}%"
        uvi = data["hourly"][i]["uvi"],
        wind = f"{data['hourly'][i]['wind_deg']} {data['hourly'][i]['wind_speed']}"
        weather_info = data["hourly"][i]['weather'][0]["main"]
        weather_detail_info = data["hourly"][i]['weather'][0]["description"]
        weather_icon = f"http://openweathermap.org/img/wn/{data['hourly'][i]['weather'][0]['icon']}@2x.png"
        hourly_results.append({
            "datetime": datetime,
            "temp": temp,
            "feels_like": feels_like,
            "humidity": humidity,
            "uvi": uvi,
            "wind": wind,
            "weather_info": weather_info,
            "weather_detail_info": weather_detail_info,
            "weather_icon": weather_icon
        })
    transform_results.append(current_result, hourly_results)
    logger.info("transfrom data complete")
    
    return transform_results
    
def load(**context):
    
    location_names = [dir.replace("./data_lake/", "") for dir in glob("./data_lake/*")]
    data = context["task_instance"].xcom_pull(task_ids="transform")
    
    insert_current_query = f"""
                    INSERT INTO {table_name}
                    ('{column1}', '{column2}', '{column3}')
                    VALUES (%s, %s, %s)
                    """ # current, hourly 테이블에 맞게 하나씩 생성
    for type in data: # current, hourly 순으로 진행
        ds_cursor.execute(insert_current_query)
    
    logger.info("Load data Complete")
    
dag = DAG(
        dag_id = "openweather_daily",
        start_date = datetime(2022,4,25),
        schedule_interval = "0 * * * *", 
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