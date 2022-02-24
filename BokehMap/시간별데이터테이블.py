#!/usr/bin/env python
# coding: utf-8

# In[ ]:



# 시간별 테이블 데이터 시각화 

def hourlyTable(hourlyDf,width=1200, height=800):

    from datetime import date
    from random import randint
    import pandas as pd 

    from bokeh.io import output_file, show
    from bokeh.layouts import widgetbox
    from bokeh.models import ColumnDataSource
    from bokeh.models.widgets import DataTable, DateFormatter, TableColumn

    hourlyDf["datetime"] = pd.to_datetime(hourlyDf["datetime"],format="%Y-%m-%d %H:%M:%S", errors = 'coerce')     # datetime 형식으로 변경 

    source = ColumnDataSource(hourlyDf)
    columns = [
            TableColumn(field="datetime", title="시간", formatter=DateFormatter(format="%Y-%m-%d %H:%M:%S")),
            TableColumn(field="name", title="지명"),
            TableColumn(field="temp", title="온도 (˚C)"),
            TableColumn(field="humidity", title="습도 (%)"),
            TableColumn(field="precipitation_probability", title="강수확률 (%)"),
            TableColumn(field="uvi", title="자외선 지수"),
            TableColumn(field="wind", title="바람 (m/s)"),
            TableColumn(field="weather", title="날씨"),
        ]

    data_table = DataTable(source=source, columns=columns,width=width, height=height)

    return widgetbox(data_table)


# In[ ]:



# mysql Connection 
def mysqlConn(host="220.78.231.223", port=3306, user="jueun", passwd="jueun", db="weather",):    
    import pymysql
    import logging

    try:
        conn_db = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset="utf8")
        return conn_db

    except Exception as ex:
        logging.error("DB Connection Issue : {}".format(ex))


# In[ ]:




def main():
    from bokeh.io import show
    import pandas as pd 

    # DB Connection
    conn_db = mysqlConn(host="220.78.231.223", port=3306, user="jueun", passwd="jueun", db="weather")

    # 시간별 데이터 
    sql = "select * from weather.hourly_weather;"
    hourlyDf = pd.read_sql_query(sql,conn_db)
    hourlyPlot = hourlyTable(hourlyDf,width=1200, height=800)
    show(hourlyPlot)
    
    conn_db.close()
    
    return hourlyPlot

if __name__ == "__main__":

    main()    

