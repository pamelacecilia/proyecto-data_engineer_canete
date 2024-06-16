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
    excel = pd.read_excel('./keyWallets.xls')
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
        df.insert(15,"walletTag", "None")
        for index, row in excel.iterrows():
            for i, line in df.iterrows():
                if line['De'] == row['wallet']:
                    line['walletTag'] = row['reason']
        return df
    except Exception as e:
        print("Error descargando datos")
        print(e)


def iniciar():
    try:
        file = json.load(open('./conn.json'))
    except Exception as e:
        print("Archivo con credenciales no encontrado")
    lastBlock = ultimoBloque(file)
    datos = obtenerDatos(file, lastBlock)
    print(datos)
if __name__ == "__main__":
    iniciar()