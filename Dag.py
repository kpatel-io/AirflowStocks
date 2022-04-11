import os
import csv
#from asyncio import FastChildWatcher
#from asyncio.tasks import _T5
from datetime import datetime, timedelta, date
from hmac import trans_5C
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import yfinance as yf
import pandas as pd

start_date = datetime.today() - timedelta(days = 2)
end_date = start_date + timedelta(days=1)

DAG_DEFAULT_ARGS = {
    'depends_on_past': False,
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': start_date,
}

dag = DAG(
    'marketvol',
    description = 'Ticker_value',
    schedule_interval = '0 18 * * 1-5',
    default_args= DAG_DEFAULT_ARGS,
)

def download_data(ticker, **args):
    variable = ticker
    x_df = yf.download(variable, start=start_date, end=end_date, interval='1m')
    x_df.to_csv(f"/home/airflow/{variable}.csv", header=['open','high','low','close','adj_close','volume'])

#Rename directory with current date bash
t0 = BashOperator(
    task_id = 'temporary_directory',
    bash_command=(f"mkdir -p /home/airflow/tmp/data/{date.today()}"),
    dag=dag,
)

t1 = PythonOperator(
    task_id = 'call_download_data_for_APPL',
    provide_context=True,
    python_callable = download_data,
    op_kwargs = {"ticker" : "APPL"},
    dag=dag,
)

t2 = PythonOperator(
    task_id = 'call_download_data_for_TSLA',
    provide_context=True,
    python_callable = download_data,
    op_kwargs = {"ticker" : "TSLA"},
    dag=dag,
)

# For Appl
t3 = BashOperator(
    task_id ='Move_to_dir',
    bash_command=f"mv  /home/airflow/APPL.csv  /home/airflow/tmp/data/{date.today()}",
    dag=dag,
)

#For TSLA
t4 = BashOperator(
    task_id ='Move_to_Directory',
    bash_command=f"mv  /home/airflow/TSLA.csv  /home/airflow/tmp/data/{date.today()}",
    dag=dag,
)

#Query for t5
def querydata( **args):
    #average for APPL symbol
    APPL_csv = pd.read_csv(f'/home/airflow/tmp/data/{date.today()}/APPL.csv', 'high', 'low')
    #average for TSLA symbol
    TSLA_csv = pd.read_csv(f'/home/airflow/tmp/data/{date.today()}/TSLA.csv', 'high', 'low')

t5 = PythonOperator(
    task_id ='query_on_both_data_files',
    python_callable = querydata,
    dag=dag,
)

# Defining depenencies
t0 >> t1
t0 >> t2
t1 >> t3
t2 >> t4
t3 >> t5 << t4


#t2 = BashOperator(task_id="anything unique", bash_operator="mkdir ...")
#t2 = BashOperator(task_id="anything unique", bash_command="mkdir ...")