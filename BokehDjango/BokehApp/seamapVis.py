#!/usr/bin/env python
# coding: utf-8

# In[64]:

# 바다 데이터 plot 
def seamapPlot(lat, lng, sDf, zoom=10, map_type='roadmap'):

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
                <span style="font-size: 17px; font-weight: bold;">수온</span>
                <span style="font-size: 15px; color: #966;">@water_temp &#8451</span>
            </div> 
            <div>
                <span style="font-size: 17px; font-weight: bold;">기온</span>
                <span style="font-size: 15px; color: #966;">@air_temp{0.00} &#8451</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">기압</span>
                <span style="font-size: 15px; color: #966;">@air_pres hPa</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">풍향</span>
                <span style="font-size: 15px; color: #966;">@wind_dir deg</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">풍속</span>
                <span style="font-size: 15px; color: #966;">@wind_speed m/s</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">염분</span>
                <span style="font-size: 15px; color: #966;">@salinity psu</span>
            </div>     
             <div>    
                <span style="font-size: 17px; font-weight: bold;">예측 조위</span>
                <span style="font-size: 15px; color: #966;">@pre_value cm</span>
            </div>     
             <div>
                <span style="font-size: 17px; font-weight: bold;">실측 조위</span>
                <span style="font-size: 15px; color: #966;">@real_value cm</span>
            </div>                                          
        </div>
        """

    p = gmap(api_key, gmap_options, title='Pays de Gex', tooltips=TOOLTIPS)

    seaSrc = ColumnDataSource(sDf)

    p.triangle(x = 'lon', y = 'lat', size=10, alpha=0.8,  source=seaSrc, legend_label='제주', color='#35B778') 
    
    p.legend.location = "top_right"
    p.legend.orientation = "horizontal"
    p.legend.click_policy="hide"

    return p


# In[50]:


# mysql Connection 
def mysqlConn(host="220.78.231.223", port=3306, user="jueun", passwd="jueun", db="weather",):    
    import pymysql
    import logging

    try:
        conn_db = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset="utf8")
        return conn_db

    except Exception as ex:
        logging.error("DB Connection Issue : {}".format(ex))
    


# In[53]:


def main():
    from bokeh.io import show
    import pandas as pd 

    # DB Connection
    conn_db = mysqlConn(host="220.78.231.223", port=3306, user="jueun", passwd="jueun", db="weather")

    # 제주도 중앙 경도 위도
    lat , lon = 33.4,126.6  

    # 바다 데이터 
    sql = "SELECT distinct d.record_time, d.water_temp, d.air_temp, d.air_pres, d.wind_dir, d.wind_speed, d.salinity, s.pre_value, s.real_value from weather.dt_0004 as d, weather.dt_0004_tidecurpre as s where d.record_time = s.record_time;"
    seaDf = pd.read_sql_query(sql,conn_db)
    seaDf["lat"]  = 33.5275 
    seaDf["lon"] =  126.543056

    seaPlot = seamapPlot(lat, lon,  seaDf[-1:], zoom=9, map_type='roadmap')
    # show(seaPlot)

    conn_db.close()
    return seaPlot

# In[ ]:


if __name__ == "__main__":

    main()    


# In[ ]:




