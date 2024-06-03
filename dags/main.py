import pandas as pd
import json, requests, psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def convertWei(wei):
    bnb = wei * (1/(10 ** 18))
    return bnb

def ultimoBloque(file):
    params = {
        'module': 'proxy',
        'action': 'eth_blockNumber',
        'apikey': file["connection"]["API_KEY"]
    }
    response = requests.get(file["connection"]["API_URL"], params=params, headers={'Accept': 'application/json'})
    data = response.json()
    block_number_hex = data['result']
    block_number = int(block_number_hex, 16)
    return block_number

def obtenerDatos(file, lastBlock):
    params = {
        'module': 'account',
        'action': 'txlistinternal',
        'startblock': str(int(lastBlock) - 10000),
        'endblock': lastBlock,
        'apikey': file["connection"]["API_KEY"]
    }    
    try:
        print("Descargando Datos")
        request = (requests.get(file["connection"]["API_URL"], params=params, headers={'Accept': 'application/json'}))
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

def conexionRS(file):
    #Crear conexion con RS
    try:
        conexion = psycopg2.connect(
            host=file["connection"]["REDSHIFT_URL"],
            dbname=file["connection"]["REDSHIFT_DB"],
            user=file["connection"]["REDSHIFT_USR"],
            password=file["connection"]["REDSHIFT_PWD"],
            port=file["connection"]["REDSHIFT_PORT"]
            )
        print("Conexion con RedShift exitosa")
        return conexion
    except Exception as e:
        print("Error al conectar a RedShift")
        print(e)

def crearTabla(conexion): 
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

def insertarDatos(conexion, datos):
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
    try:
        file = json.load(open('./conn.json'))
    except Exception as e:
        print("Archivo con credenciales no encontrado")
    lastBlock = ultimoBloque(file)
    datos = obtenerDatos(file, lastBlock)
    conexion = conexionRS(file)
    crearTabla(conexion)
    insertarDatos(conexion, datos)

if __name__ == "__main__":
    iniciar()