from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from pymongo import MongoClient
import pandas as pd
import MySQLdb
from sqlalchemy import create_engine
import requests
from io import StringIO 
import boto3



env_dados = pd.read_csv('./data/aws.csv')

env_dict = env_dados.to_dict('records')

env = env_dict[0]

YOUR_S3_BUCKET = env['YOUR_S3_BUCKET']
AWS_ACCESS_KEY_ID = env['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = env['AWS_SECRET_ACCESS_KEY']
USER_DB = env['USER_DB']
PWD_DB = env['PWD_DB']
HOST_DB = env['HOST_DB']

default_args = {
    'owner': 'Carlos Bruno',
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 5, 14,50),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "treino-01",
    description="Baixar e salvar dados", 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=1)
)

def connect_to_db():    
    database = 'bootcamp-ed'
    engine = create_engine(f'mysql://{USER_DB}:{PWD_DB}@{HOST_DB}/{database}', isolation_level="READ UNCOMMITTED")
    return engine

def store_data_in_db(df, table):
    conn = connect_to_db()
    df.to_sql(table, con=conn, if_exists='append', index=False, index_label='id')

def save_to_s3(df, key):
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3',
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key= AWS_SECRET_ACCESS_KEY)
    s3_resource.Object(YOUR_S3_BUCKET, key).put(Body=csv_buffer.getvalue())

def get_data_from_mongo():
    url = 'mongodb+srv://estudante_igti:SRwkJTDz2nA28ME9@unicluster.ixhvw.mongodb.net'
    conn = MongoClient(url)
    db = conn.ibge
    collection = db.pnadc20203
    data = collection.find({
        'sexo': 'Mulher',
        'idade': {
            '$gte': 20,
            '$lte': 40
        }
    })

    df = pd.DataFrame( list( data ) )

    customFile = 'ibge_women.csv'

    df.to_csv(f"./data/{customFile}",index=False)
    
    # upload to S3 bucket
    key = customFile
    save_to_s3(df, key)
    
    
    


def extract_and_save():
    df_estados = pd.read_csv('./data/estados.csv')

    
    
    
    df_dados = pd.read_csv('./data/ibge_women.csv')
    conn = connect_to_db()

    

    df2 = df_dados

    df2['sexo'] = df2['sexo'].map(lambda sexo: "F" if sexo == 'Mulher' else 'M')

    df2['horastrab'] = df2['horastrab'].fillna(0)
    df2['renda'] = df2['renda'].fillna(0)

    df_cores = df2['cor']
    df_cor = df_cores.drop_duplicates()

    df_ocups = df2['ocup']
    df_ocup = df_ocups.drop_duplicates()

    df_trabs = df2['trab']
    df_trab = df_trabs.drop_duplicates()

    df_estados = df_estados.drop('id', 1)   
    store_data_in_db(df_estados,'estados')
    store_data_in_db(df_cor,'cor')
    store_data_in_db(df_ocup,'ocup')
    store_data_in_db(df_trab,'trab')


    df_estados_db = pd.read_sql('estados', conn)
    df_cor_db = pd.read_sql('cor', conn)
    df_ocup_db = pd.read_sql('ocup', conn)
    df_trab_db = pd.read_sql('trab', conn)

    dict_estado = df_estados_db.set_index(df_estados_db.nome)['id'].to_dict()
    dict_cor = df_cor_db.set_index(df_cor_db.cor)['id'].to_dict()
    dict_ocup = df_ocup_db.set_index(df_ocup_db.ocup)['id'].to_dict()
    dict_trab = df_trab_db.set_index(df_trab_db.trab)['id'].to_dict()

    df2.uf = df2.uf.map(dict_estado)
    df2.cor = df2.cor.map(dict_cor)
    df2.ocup = df2.ocup.map(dict_ocup)
    df2.trab = df2.trab.map(dict_trab)
    df2 = df2.drop('_id', 1)
    store_data_in_db(df2,'dados')

hello_bash = BashOperator(
    task_id="hello-bash",
    bash_command='echo "Hello Airflow from bash"',
    trigger_rule="all_done",
    dag=dag
)

def get_data_uf_from_ibge():
    data = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/estados").json()
    df_estados = pd.DataFrame.from_dict(data)  
    df_estados['regiao'] = df_estados['regiao'].apply(lambda uf: uf['nome'])
    
    customFile = 'estados.csv'
    df_estados.to_csv(f'./data/{customFile}',index=False)


    # upload to S3 Bucket
    key = customFile
    save_to_s3(df_estados,key)
    
    

get_data_mongo = PythonOperator(
    task_id='get-data-from-mongo',
    python_callable=get_data_from_mongo,
    dag=dag
)

get_data_uf = PythonOperator(
    task_id='get-data-uf',
    python_callable=get_data_uf_from_ibge,
    dag=dag
)

get_extract_and_load = PythonOperator(
    task_id='get-extract-and-load',
    python_callable=extract_and_save,
    dag=dag
)

hello_bash >> get_data_mongo >> get_data_uf >> get_extract_and_load