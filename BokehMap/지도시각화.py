#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def mapPlot(lat, lng, wDf,sDf, zoom=10, map_type='roadmap'):

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

    weatherSrc = ColumnDataSource(wDf)
    seaSrc = ColumnDataSource(sDf)

    
    p.circle(x = 'lon', y = 'lat', size=10, alpha=0.8,  source=weatherSrc, legend_label='날씨', color='blue')
    p.circle(x = 'lon', y = 'lat', size=10, alpha=0.8,  source=seaSrc, legend_label='바다', color='yellow')
    
    p.legend.location = "top_right"
    p.legend.orientation = "horizontal"
    p.legend.click_policy="hide"

    return p


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

    # 지도 데이터 
    sql = "select * from weather.current_weather;" 
    mapDf = pd.read_sql_query(sql,conn_db)
    lat , lon = 33.4,126.6 # 제주도 중앙 경도 위도 
    gmPlot = mapPlot(lat, lon, mapDf, map_type='roadmap', zoom=9) # 바다 데이터 추가하기 
    show(gmPlot)


# In[ ]:


if __name__ == "__main__":

    main()    

