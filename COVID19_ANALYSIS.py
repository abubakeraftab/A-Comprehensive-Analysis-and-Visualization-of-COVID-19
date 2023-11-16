#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np 
import requests
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression


# In[3]:


def extract_data():

    response=requests.get("https://api.covidtracking.com/v1/us/daily.csv")
    
    with open('covid.csv','wb') as f:
        f.write(response.content)
        f.close()

       


# In[ ]:


# def get_data():
#     extract_data()
#     return pd.read_csv('covid.csv')


# In[4]:


def pre_processing():
    
    df=pd.read_csv('covid.csv')
    df=df[df.states==56]
    
    df=df.drop(columns=["recovered", "lastModified", "states", "dateChecked", "total", "posNeg", "hospitalized"])
    
    df=df[["hash", "date", "positive", "negative", "positiveIncrease", "negativeIncrease", "pending", "hospitalizedCurrently", "hospitalizedIncrease", "hospitalizedCumulative",
             "inIcuCurrently", "inIcuCumulative", "onVentilatorCurrently", "onVentilatorCumulative", "totalTestResults", "totalTestResultsIncrease",
             "death", "deathIncrease" ]]
    
    df.rename(columns={
        "positive":"PCRTestPositive",
        "negative":"PCRTestNegative"
    },inplace=True)
    
    df["date"]=df.date.astype("str")
    df["date"]=pd.to_datetime(df.date.str[0:4]+"-"+df.date.str[4:6]+"-"+df.date.str[6:8])
    
    df=df.dropna()
    df.to_csv("cleaneddata.csv")
    
    r_squared=pd.DataFrame(["hospitalizedCurrently", "inIcuCurrently", "onVentilatorCurrently"],columns=["Features"])
    r_squared["R_Score"]="None"
    
    y=np.array(df.loc[:,'deathIncrease']).reshape(-1,1)
    
    i=0
    for col in ["hospitalizedCurrently", "inIcuCurrently", "onVentilatorCurrently"]:
        x=np.array(df.loc[:,col]).reshape(-1,1)
        x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.3,random_state=42)
        
        model=LinearRegression()
        model.fit(x_train,y_train)
        rsquared=model.score(x_test,y_test)
        r_squared.R_Score[i]=rsquared
        i=i+1
    r_squared.to_csv("R2_Score.csv")
    
    return df




# In[5]:


df1=pre_processing()
df1.head()


# In[7]:


from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
'covid-analysis.json')

project_id = 'covid-analysis-405023'
bigquery_client = bigquery.Client(credentials= credentials,project=project_id)


# bigquery_client = bigquery.Client()


def get_or_create_dataset(dataset_name: str) -> bigquery.dataset.Dataset:
    print("Fetching Dataset")

    try:
        dataset1 = bigquery_client.get_dataset(dataset_name)
        print("Done")
        print(dataset1.self_link)
        return dataset1


    except Exception as e:
        if e.code == 404:
            print("CREATING DATASET")
            bigquery_client.create_dataset(dataset_name)
            dataset1 = bigquery_client.get_dataset(dataset_name)
            print("Done")
            print(dataset1.self_link)
            return dataset1
        else:
            print(e)


def get_or_create_table(dataset_name:str,table_name:str)->bigquery.table.Table:
    
    dataset=get_or_create_dataset(dataset_name)
    print("DATASET ID", dataset.dataset_id)
    project=dataset.project
    dataset=dataset.dataset_id
    table_id=project+"."+dataset+"."+table_name
    print("Fetching Table")
    try:
        table=bigquery_client.get_table(table_id)
        print("Done")
        print(table.self_link)
#         return table
    except Exception as e:
        if e.code==404:
            bigquery_client.create_table(table_id)
            table=bigquery_client.get_table(table_id)
            print("Done")
            print(table.self_link)
        else:
            print(e)
        
    finally:
        return table
            
def load_to_BigQuery(dataset_name="covid_analysis", table_name="covid_data"):
    table=get_or_create_table(dataset_name=dataset_name,table_name=table_name)
    job_config=bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("hash","String"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("PCRTestPositive", "INTEGER"),
            bigquery.SchemaField("PCRTestNegative", "INTEGER"),
            bigquery.SchemaField("positiveIncrease", "INTEGER"),
            bigquery.SchemaField("negativeIncrease", "INTEGER"),
            bigquery.SchemaField("pending", "INTEGER"),
            bigquery.SchemaField("hospitalizedCurrently", "INTEGER"),
            bigquery.SchemaField("hospitalizedIncrease", "INTEGER"),
            bigquery.SchemaField("hospitalizedCumulative", "INTEGER"),
            bigquery.SchemaField("inIcuCurrently", "INTEGER"),
            bigquery.SchemaField("inIcuCumulative", "INTEGER"),
            bigquery.SchemaField("onVentilatorCurrently", "INTEGER"),
            bigquery.SchemaField("onVentilatorCumulative", "INTEGER"),
            bigquery.SchemaField("totalTestResults", "INTEGER"),
            bigquery.SchemaField("totalTestResultsIncrease", "INTEGER"),
            bigquery.SchemaField("death", "INTEGER"),
            bigquery.SchemaField("deathIncrease", "INTEGER")
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    
    )
    data_to_load=pd.read_csv("cleaneddata.csv",index_col=0)
    data_to_load= data_to_load.replace(np.nan,0)
    
    job=bigquery_client.load_table_from_dataframe(data_to_load,table,job_config=job_config)
    job.result()
    
    print("Loaded {} rows into {}:{}".format(job.output_rows,dataset_name,table_name))
    
            


# In[12]:


# pip install "apache-airflow[celery]==2.7.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"
# !pip install "apache-airflow==2.7.3" apache-airflow-providers-google==10.1.0

# !pip uninstall celery
# !pip install celery==5.0.5
# !pip install --upgrade helper
# !pip install pycopy-grp



# In[5]:


from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import datetime
import sys
import os


# from helper import extract_data, pre_processing,load_to_BigQuery

os.environ["GOOGLE_CREDS"]="covid-analysis.json"
load_dotenv("covid-analysis.json")
def_args={
    'start_date':datetime.datetime(2023,11,15)
}


with DAG (dag_id="COVID19_ANALYSIS_dag",catchup=False, schedule_interval="@Daily",default_args=def_args) as dag:
    
    extract_data=PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )
    
    transform_data=PythonOperator(
        task_id="pre_processing",
        python_callable=pre_processing
    
    )
    load_data=PythonOperator(
        task_id="load",
        python_callable=load_to_BigQuery,
        op_kwargs={
            "dataset_name":"covid_analysis",
            "table_name":"covid_data"
            
        }
    )
    extract_data>>transform_data>>load_data
    

