"""Main function for Extract/Transform portion of the pipeline.
The main function contains many little sub-functions"""

import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, DataFrame,SparkSession, SQLContext, Row
from datetime import datetime
import time
import operator
import pandas as pd
import json 

#initialize spark session
sc = SparkContext(conf=SparkConf().setAppName("videos"))
sqlContext = SQLContext(sc)
spark= SparkSession.builder.appName("videos").getOrCreate() 

#set up data parameters
visits='visits.csv'
providers='providers.csv'
visits_csv=pd.read_csv(visits,delimiter=',', header=None)
df = visits_csv.rename(columns={0: 'visit_id', 1: 'provider_id', 2: 'date'})
visits_df1= df[['visit_id','provider_id','date']]
visits_df= df[['visit_id','provider_id']]
providers_df=pd.read_csv(providers,delimiter='|')

df=pd.merge(providers_df,visits_df, on='provider_id')
df1=pd.merge(providers_df,visits_df1, on='provider_id')
#df1=df1.iloc[:80]

def partitionBy_specialty(df):
    total=[]
    a=0
    c=0
    for index, row in df.iterrows():
        if row['provider_id']!=a:
            a=row['provider_id']
            total.append(c)
            c=1
        else:
            c+=1
            total.append(0)
    total=total[1:]
    total.append(c)
    df['total']=total
    df=df[['provider_specialty','provider_id', 'first_name','middle_name','last_name','visit_id','total']]
    df = df.drop(df[df['total'] == 0].index)  
    df1=spark.createDataFrame(df)
    #df1.show()
    df1.write.format('json').option('header',True).partitionBy('provider_specialty').mode('append').save("provider_byspecialty.json")

def partitionBy_month(df1):
    month=[]
    mix=[]
    for index, row in df1.iterrows():
        a=row['date']
        a=a[5:7]
        m=row['provider_id']
        m=str(m)+str(a)
        month.append(a)
        mix.append(m)
    df1['month']=month
    df1['mix']=mix
    df1=df1[['provider_id','month','mix']]
    df1=df1.sort_values(by=['mix'], ascending=True)
    
    total=[]
    a=0
    c=0
    for index, row in df1.iterrows():
        if row['mix']!=a:
            a=row['mix']
            total.append(c)
            c=1
        else:
            c+=1
            total.append(0)

    total=total[1:]
    total.append(c)
    df1['total']=total
    df1=df1[['provider_id','month','total']]
    df1 = df1.drop(df1[df1['total'] == 0].index)  
    df1=spark.createDataFrame(df1)
    #df1.show()
    df1.write.format('json').option('header',True).mode('append').save("provider_bymonth.json")

if __name__ == '__main__':
    partitionBy_specialty(df)
    partitionBy_month(df1)
