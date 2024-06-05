from datetime import timedelta,datetime
from airflow import DAG
import pandas as pd
import json, requests, psycopg2, os
from psycopg2.extras import execute_values
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

dag_path = os.getcwd()
default_args = {
    'start_date': datetime(2024, 5, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def convertWei(wei):
    bnb = wei * (1/(10 ** 18))
    return bnb

def ultimoBloque():
    params = {
        'module': 'proxy',
        'action': 'eth_blockNumber',
        'apikey': Variable.get("API_KEY")
    }
    response = requests.get(Variable.get("API_URL"), params=params, headers={'Accept': 'application/json'})
    data = response.json()
    block_number_hex = data['result']
    block_number = int(block_number_hex, 16)
    return block_number

def obtenerDatos(lastBlock):
    params = {
        'module': 'account',
        'action': 'txlistinternal',
        'startblock': str(int(lastBlock) - 10000),
        'endblock': lastBlock,
        'apikey': Variable.get("API_KEY")
    }    
    try:
        print("Descargando Datos")
        request = (requests.get(Variable.get("API_URL"), params=params, headers={'Accept': 'application/json'}))
        results = pd.json_normalize((request.json()["result"]), meta=['blockNumber', 'timeStamp', 'hash', 'from', 'to', 'value', 'contractAddress', 'input', 'type', 'gas', 'gasUsed', 'traceId', 'isError', 'errCode'])
        results.columns = ['Numero de Bloque', 'Marca de Tiempo', 'Hash', 'De', 'A', 'Valor', 'Contrato', 'Entrada', 'Tipo', 'Gas Total', 'Gas Usado', 'Id de Traza', 'Error', 'Codigo de Error']
        print("Datos Descargados")
        df = pd.DataFrame(results)
        #Convierto de wei a BNB para achicar los valores
        df['Valor'] = df['Valor'].apply(lambda x: convertWei(int(x)))
        df.insert(14,"Creado",datetime.now())
        #print("Resultados: ", df)
        return df
    except Exception as e:
        print("Error descargando datos")
        print(e)

def conexionRS():
    #Crear conexion con RS
    try:
        conexion = psycopg2.connect(
            host=Variable.get("REDSHIFT_URL"),
            dbname=Variable.get("REDSHIFT_DB"),
            user=Variable.get("REDSHIFT_USR"),
            password=Variable.get("REDSHIFT_PWD"),
            port=Variable.get("REDSHIFT_PORT")
            )
        print("Conexion con RedShift exitosa")
        return conexion
    except Exception as e:
        print("Error al conectar a RedShift")
        print(e)

def crearTabla():
    conexion = conexionRS() 
   #Crear tabla y vaciar si existe
    print("Creando Tabla en RedShift")
    with conexion.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pcc_pam_coderhouse.bloques
            (
            bloque INTEGER,
            time_stamp INTEGER,
            hash VARCHAR(255) primary key,
            from_wallet VARCHAR(255),
            to_wallet VARCHAR(255),
            value DOUBLE PRECISION,
            contract_address VARCHAR(255),
            input VARCHAR(100),
            type VARCHAR(255),
            gas INTEGER,
            gas_used INTEGER,
            trace_id VARCHAR(100),
            is_error INTEGER,
            err_code VARCHAR(100),
            creado date
            )
        """)
        conexion.commit()
        cur.execute("Truncate table bloques")
        count = cur.rowcount
        print("Tabla creada")

def insertarDatos(datos):
    conexion = conexionRS() 
    print("Insertando datos obtenidos")
    with conexion.cursor() as cur:
        datos['Creado'] = datos['Creado'].apply(lambda x: datetime.now())
        execute_values(
            cur,
            '''
            INSERT INTO bloques (bloque, time_stamp, hash, from_wallet, to_wallet, value, contract_address, input, type, gas, gas_used, trace_id, is_error, err_code,creado)
            VALUES %s
            ''',
            datos.values
        )
        conexion.commit()
        print("Datos insertados con exito")
    cur.close()
    conexion.close()

def iniciar():
    lastBlock = ultimoBloque()
    datos = obtenerDatos(lastBlock)
    insertarDatos(datos)  

first_dag = DAG(
    dag_id='Blockchain',
    default_args=default_args,
    description='Add last 1000 blocks from BCS',
    start_date=datetime(2024,6,1),
    schedule_interval='@daily',
    catchup=False
)

task_1 = BashOperator(
    task_id='log_start',
    bash_command='echo Starting...'
)

task_2 = PythonOperator(
    task_id='Create_Table',
    python_callable=crearTabla,
    dag=first_dag,
)

task_3 = PythonOperator(
    task_id='main_task',
    python_callable=iniciar,
    dag=first_dag,
)

task_4 = BashOperator(
    task_id='log_end',
    bash_command='echo Process Complete...'
)

task_1 >> task_2 >> task_3 >> task_4