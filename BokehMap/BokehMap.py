# google Map 시각화 

def mapPlot(lat, lng, df,zoom=10, map_type='roadmap'):

    from bokeh.io import output_file, show
    from bokeh.models import ColumnDataSource, GMapOptions
    from bokeh.plotting import gmap
    import pandas as pd 
    import os 


    os.environ["GOOGLE_API_KEY"] = 'AIzaSyCHqZYVSJau7_qVmDdAtG5BY3v7sRa4eC0' # google api key  
    api_key = os.environ['GOOGLE_API_KEY']

    gmap_options = GMapOptions(lat=lat, lng=lng, map_type=map_type, zoom=zoom)

    TOOLTIPS = """
        <div>
            <div>
                <img
                    src="@weather_icon" height="50" alt="@weather_icon" width="50"
                    style="float: left; margin: 40px 40px 40px 40px;"
                    border="2"
                ></img>
            </div>
            <div>
                <span style="font-size: 15px; font-weight: bold;">지명</span>
                <span style="font-size: 17px; color: #966;">@name</span>
            </div> 
            <div>
                <span style="font-size: 17px; font-weight: bold;">온도</span>
                <span style="font-size: 15px; color: #966;">@temp &#8451</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">체감온도</span>
                <span style="font-size: 15px; color: #966;">@feels_like  &#8451</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">습도</span>
                <span style="font-size: 15px; color: #966;">@humidity %</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">자외선 지수</span>
                <span style="font-size: 15px; color: #966;">@uvi</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">바람</span>
                <span style="font-size: 15px; color: #966;">@wind{0.00} m/s</span>
            </div>   
            <div>
                <span style="font-size: 17px; font-weight: bold;">날씨</span>
                <span style="font-size: 15px; color: #966;">@weather</span>
            </div>  
            <div>
                <span style="font-size: 17px; font-weight: bold;">상태</span>
                <span style="font-size: 15px; color: #966;">@description</span>
            </div>                                        
        </div>
        """

    p = gmap(api_key, gmap_options, title='Pays de Gex', tooltips=TOOLTIPS)

    source = ColumnDataSource(df)

    
    for _,col in zip(range(3),["yellow","blue","purple"]):
        p.circle(x = 'lon', y = 'lat', size=10, alpha=0.8,  source=source, legend_label=col, color=col)
    # p.circle(x = 'lon', y = 'lat', size=10, alpha=0.5,  color = "yellow",source=source)
    
    p.legend.location = "top_right"
    p.legend.orientation = "horizontal"
    p.legend.click_policy="hide"

    return p

# 일별 테이블 데이터 시각화 

def dailyTable(dailyDf,width=1200, height=800):

    from datetime import date
    from random import randint

    from bokeh.io import output_file, show
    from bokeh.layouts import widgetbox
    from bokeh.models import ColumnDataSource
    from bokeh.models.widgets import DataTable, DateFormatter, TableColumn

    dailyDf["datetime"] = pd.to_datetime(dailyDf["datetime"],format="%Y-%m-%d", errors = 'coerce')     # datetime 형식으로 변경 

    source = ColumnDataSource(dailyDf)
    columns = [
            TableColumn(field="datetime", title="시간", formatter=DateFormatter(format="%Y-%m-%d")),
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

# 시간별 테이블 데이터 시각화 

def hourlyTable(hourlyDf,width=1200, height=800):

    from datetime import date
    from random import randint

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


# mysql Connection 
def mysqlConn(host="220.78.231.223", port=3306, user="jueun", passwd="jueun", db="weather",):    
    import pymysql
    import logging

    try:
        conn_db = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset="utf8")
        return conn_db

    except Exception as ex:
        logging.error("DB Connection Issue : {}".format(ex))
    

def main():
    from bokeh.io import show
    import pandas as pd 

    # DB Connection
    conn_db = mysqlConn(host="220.78.231.223", port=3306, user="jueun", passwd="jueun", db="weather")

    # 지도 데이터 
    sql = "select * from weather.current_weather;" 
    mapDf = pd.read_sql_query(sql,conn_db)
    lat , lon = 33.4,126.6 # 제주도 중앙 경도 위도 
    gmPlot = mapPlot(lat, lon, mapDf, map_type='roadmap', zoom=9)
    show(gmPlot)

    # 일별 데이터 
    sql = "select * from weather.daily_weather;"
    dailyDf = pd.read_sql_query(sql,conn_db)
    dailyPlot = dailyTable(dailyDf,width=1200, height=800) 
    show(dailyPlot)


    # 시간별 데이터 
    sql = "select * from weather.hourly_weather;"
    hourlyDf = pd.read_sql_query(sql,conn_db)
    hourlyPlot = hourlyTable(hourlyDf,width=1200, height=800)
    show(hourlyPlot)
    
    conn_db.close()
    
    return gmPlot, dailyPlot, hourlyPlot



if __name__ == "__main__":

    main()    