# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf
from statsmodels.tsa.stattools import adfuller as ADF
forrecastnum=5
import pymysql.cursors
import time
date=time.localtime()
nt="%Y-%m"
date1=time.strftime(nt,date)
nt="%Y-%m-%d"
date2=time.strftime(nt,date)
connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
try:
    with connection.cursor() as cursor:
        sql="select name,space from shucaiapp_monthdate group by name,space"
        cursor.execute(sql)
        name=cursor.fetchall()
        connection.commit()
finally:
    connection.close()
for name in name:
    a=name[0]
    b=name[1]
    connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
    try:
        with connection.cursor() as cursor:
            sql="select average from shucaiapp_monthdate where name='%s' and space='%s'\
            and date <='%s' order by date "%(a,b,date1)
            cursor.execute(sql)
            data=cursor.fetchall()
            columnDes = cursor.description
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]
            data = pd.DataFrame([list(i) for i in data],columns=columnNames)
            connection.commit()
    finally:
        connection.close()
    plt.rcParams['font.sans-serif'] = ['SimHei']#设置字体为SimHei显示中文
    plt.rcParams['axes.unicode_minus'] = False#设置正常显示字符
    data.plot()
    plt.title('Time Series')
    plt.show()
    plot_acf(data)
    plt.show()
    D_data=data.diff(periods=1).dropna()
    D_data.columns=[u'均价差分']
    D_data.plot()
    plt.show()
    plot_acf(D_data).show()
    plot_pacf(D_data).show()
    from statsmodels.stats.diagnostic import acorr_ljungbox
    from statsmodels.tsa.arima_model import ARIMA
    data['average'] = data['average'].astype(float)
    pmax=int(len(D_data)/10)
    qmax=int(len(D_data)/10)
    bic_matrix=[]
    for p in range(pmax+1):
        tmp=[]
        for q in range(qmax+1):
            try:
                tmp.append(ARIMA(data,(p,1,q)).fit().bic)
            except:
                tmp.append(None)
        bic_matrix.append(tmp)
    bic_matrix=pd.DataFrame(bic_matrix)
    p,q=bic_matrix.stack().astype('float64').idxmin()
    model=ARIMA(data,(p,1,q)).fit()
    model.summary2()
    forecast=model.forecast(3)
    i=0
    for forecast in forecast[0]:
        connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
        try:
            with connection.cursor() as cursor:
                sql="replace into shucaiapp_monthdate values('%s','%s',(select DATE_Add('%s',INTERVAL '%s' month )),'%s','city')"%(a,forecast,date2,i,b,)
                cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()
        i=i+1
connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
try:
    with connection.cursor() as cursor:
        sql="select name,space from shucaiapp_weekdate group by name,space limit 1"
        cursor.execute(sql)
        data=cursor.fetchall()
        connection.commit()
finally:
    connection.close()
for data in data:
    a=data[0]
    b=data[1]
    connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
    try:
        with connection.cursor() as cursor:
            sql="select average from shucaiapp_weekdate where name='%s' and space='%s'\
            and weeksum <=truncate((datediff('%s','2017-01-01')-1)/7,0)+1 order by weeksum "%(a,b,date2)
            cursor.execute(sql)
            data=cursor.fetchall()
            columnDes = cursor.description
            columnNames = [columnDes[i][0] for i in range(len(columnDes))]
            data = pd.DataFrame([list(i) for i in data],columns=columnNames)
            connection.commit()
    finally:
        connection.close()
    plt.rcParams['font.sans-serif'] = ['SimHei']#设置字体为SimHei显示中文
    plt.rcParams['axes.unicode_minus'] = False#设置正常显示字符
    data.plot()
    plt.title('Time Series')
    plt.show()
    plot_acf(data)
    plt.show()
    D_data=data.diff(periods=1).dropna()
    D_data.columns=[u'均价差分']
    D_data.plot()
    plt.show()
    plot_acf(D_data).show()
    plot_pacf(D_data).show()
    from statsmodels.stats.diagnostic import acorr_ljungbox
    from statsmodels.tsa.arima_model import ARIMA
    data['average'] = data['average'].astype(float)
    pmax=int(len(D_data)/10)
    qmax=int(len(D_data)/10)
    bic_matrix=[]
    for p in range(pmax+1):
        tmp=[]
        for q in range(qmax+1):
            try:
                tmp.append(ARIMA(data,(p,1,q)).fit().bic)
            except:
                tmp.append(None)
        bic_matrix.append(tmp)
    bic_matrix=pd.DataFrame(bic_matrix)
    p,q=bic_matrix.stack().astype('float64').idxmin()
    model=ARIMA(data,(p,1,q)).fit()
    model.summary2()
    forecast=model.forecast(5)
    i=0
    for forecast in forecast[0]:
        i=i+1
        connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
        try:
            with connection.cursor() as cursor:
                sql="replace into shucaiapp_weekdate values('%s','%s',truncate((datediff((select now()),'2017-01-01')-1)/7,0)+'%s' ,'%s','city')"%(a,forecast,i,b,)
                cursor.execute(sql)
                connection.commit()
        finally:
            connection.close()





























'''
connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
try:
    with connection.cursor() as cursor:
        sql="select name,space from shucaiapp_weekdate group by name,space"
        cursor.execute(sql)
        data=cursor.fetchall()
        connection.commit()
finally:
    connection.close()
for data in data:
    a=data[0]
    b=data[1]
    connection=pymysql.connect(host='127.0.0.1', user='root',password='@Xiaoyu5211314',db='mywork',charset='utf8')
    try:
        with connection.cursor() as cursor:
            sql="select weeksum,average from shucaiapp_weekdate where name='%s' and space='%s'\
            and weeksum < truncate((datediff('%s','2017-01-01')-1)/7,0)+1 order by weeksum "%(a,b,date2)
            cursor.execute(sql)
            shuju=cursor.fetchall()
            connection.commit()
    finally:
        connection.close()
    
'''
