#!/usr/bin/env python
# coding: utf-8

# In[4]:


# 일 별 테이블 데이터 시각화 
# customJs -> datePicker 사용 

def datePickerTable(hourlyDf,width=1200, height=800):
    from datetime import date
    from random import randint
    from bokeh.layouts import widgetbox
    from bokeh.models import ColumnDataSource, DataTable, TableColumn, CustomJS, DateFormatter, DatePicker
    from bokeh.io import show, output_file
    from bokeh.layouts import row, column, layout
    import pandas as pd

    hourlyDf["timestamp"] = pd.to_datetime(hourlyDf["datetime"],format="%Y-%m-%d %H:%M:%S", errors = 'coerce')     # datetime 형식으로 변경 
    hourlyDf['datetime'] = hourlyDf.apply(lambda x: x['datetime'].split()[0],axis=1) # date 추출 -> 일별이면 안해줘도 됨 

    # src1 : 원본 데이터 
    src1 = ColumnDataSource(hourlyDf)

    # src2 : 수정 데이터 (날짜별 수정)
    df2 = pd.DataFrame(columns = ['timestamp', 'name','temp', 'humidity', 'precipitation_probability', 'uvi', 'wind','weather'])
    src2 = ColumnDataSource(df2)
    columns = [
            TableColumn(field="timestamp", title="시간", formatter=DateFormatter(format="%Y-%m-%d %H:%M:%S")),
            TableColumn(field="name", title="지명"),
            TableColumn(field="temp", title="온도 (˚C)"),
            TableColumn(field="humidity", title="습도 (%)"),
            TableColumn(field="precipitation_probability", title="강수확률 (%)"),
            TableColumn(field="uvi", title="자외선 지수"),
            TableColumn(field="wind", title="바람 (m/s)"),
            TableColumn(field="weather", title="날씨"),
        ]

    # src3 : src2 초기화 데이터 
    df3 = pd.DataFrame(columns =  ['timestamp', 'name','temp', 'humidity', 'precipitation_probability', 'uvi', 'wind','weather'])
    src3 = ColumnDataSource(df3)

    # callback 함수 : 
    callback = CustomJS(args=dict(src1=src1, src2=src2, src3=src3), code='''
        src2.data = src3.data
        var data = src1.data;
        var f = cb_obj.value;
        var date = data['datetime'];
        var columns =  ['timestamp', 'name','temp', 'humidity', 'precipitation_probability', 'uvi', 'wind','weather'];

        for (var i = 0; i < date.length; i++) {
            if(data['datetime'][i] == f){

                for(var j=0; j < columns.length; j++){
                    src2.data[columns[j]].push(src1.data[columns[j]][i]);
                }
            }
        }

        delete src2.data['index']
        console.log(src2.data)

        src2.change.emit();

        ''')

    # src2로 DataTable 생성
    data_table= DataTable(source=src2, columns=columns,width=1200, height=800)  
  
    # datePicker 범위 지정 
    date_picker = DatePicker(title='날짜 선택', value=max(hourlyDf["datetime"]), min_date=min(hourlyDf["datetime"]), max_date=max(hourlyDf["datetime"]))

    # 날짜 고를 때 callback 함수 호출
    date_picker.js_on_change("value",callback) 

    # # 위/아래로 datePicker, dataTable layout 생성 
    layout = column(date_picker, data_table)
    show(layout)

    return layout


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
 
    sql = "select * from weather.hourly_weather;"
    dPickerDf = pd.read_sql_query(sql,conn_db)
    datePickPlot = datePickerTable(dPickerDf,width=1200, height=800)
    
    show(datePickPlot)
    conn_db.close()
    
    return datePickPlot


# In[ ]:


if __name__ == "__main__":

    main()    

