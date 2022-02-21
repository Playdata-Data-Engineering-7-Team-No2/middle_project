import requests
import pymysql
import logging
import os
import json
import configparser

from datetime import datetime

# init configuration
config = configparser.ConfigParser()
config.read("./config/config.ini")

def get_lat_lon():

    try:
        conn_db = pymysql.connect(host="localhost", port=3306, user=config["DB"]["USERNAME"], passwd=config["DB"]["PASSWORD"], db="weather", charset="utf8")
        cursor = conn_db.cursor()
    except Exception as ex:
        logging.error("DB Connection Issue : {}".format(ex))
    
    sql = "SELECT * FROM location"
    cursor.execute(sql)

    data = cursor.fetchall()

    cursor.close()
    conn_db.close()

    return data

def main():

    api_url = "https://api.openweathermap.org/data/2.5/onecall"
    data = get_lat_lon()

    # file_name 생성
    now = datetime.now()
    file_name = "{}_{}_{}_{}.json".format(str(now.year), str(now.month), str(now.day), str(now.hour))

    for item in data:
        
        data_dir_path = "./data_lake/{}".format(item[0])

        if not os.path.exists(data_dir_path):
            os.mkdir(data_dir_path)
        
        api_param = {
            "lat" : item[1],
            "lon" : item[2],
            "appid" : config["API"]["API_KEY"]
        }    

        try:
            res = requests.get(api_url, params = api_param) 
            
            with open("{}/{}".format(data_dir_path, file_name), "w") as f:
                json.dump(res.json(), f)
        except Exception as ex:
            logging.critical("Request API Issue : {}".format(ex))


if __name__ == "__main__":

    main()
