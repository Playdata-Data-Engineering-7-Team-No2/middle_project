## import 
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, GMapOptions
from bokeh.plotting import gmap
import pandas as pd 
import os 

def plot(lat, lng, df,zoom=10, map_type='roadmap'):

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
                <span style="font-size: 17px; font-weight: bold;">자외선</span>
                <span style="font-size: 15px; color: #966;">@uvi</span>
            </div>
            <div>
                <span style="font-size: 17px; font-weight: bold;">바람</span>
                <span style="font-size: 15px; color: #966;">@wind_deg @wind_speed{0.00} m/s</span>
            </div>   
            <div>
                <span style="font-size: 17px; font-weight: bold;">날씨</span>
                <span style="font-size: 15px; color: #966;">@weather</span>
            </div>  
            <div>
                <span style="font-size: 17px; font-weight: bold;">description</span>
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
    show(p)
    return p



def main():

    # txt 데이터 - 위도, 경도, 지명 
    lat, lon , city = [], [], []

    with open("./jeju.txt", "r",encoding='UTF8') as f:
        lines = f.readlines()

        for l in lines:
            cit, la, lo = l.strip().split(",")
            lat.append(float(la))
            lon.append(float(lo))
            city.append(str(cit))
            
    tmpDf = pd.DataFrame({"lat":lat, "lon":lon, "city":city})
    tmpDf = tmpDf.sort_values(by=['city'],axis=0) # name 기준 sort 
    tmpDf = tmpDf.reset_index(drop=True) # 인덱스 초기화 

    # csv 데이터 - 지명, 온도, 습도.. (위도, 경도 존재 x)
    df = pd.read_csv('weather.csv', encoding='utf-8')
    df = df.sort_values(by=["name"],axis=0)
    df = df.reset_index(drop=True) # 인덱스 초기화 

    # csv 데이터와 txt 데이터 인덱스 기준 concat 
    jejuSample = pd.concat([df,tmpDf], axis = 1) 

    # print(jejuSample.head())

    lat , lon = 33.4,126.6 # 제주도 중앙 경도 위도 
    p = plot(lat, lon, jejuSample, map_type='roadmap', zoom=9)

    return p

if __name__ == "__main__":

    main()    
