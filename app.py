import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
import numpy as np

app = Flask(__name__)
CORS(app) 

app.config['JSON_AS_ASCII'] = False

df_dados = pd.DataFrame()

def to_numeric_br(series):
    return pd.to_numeric(series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.'), errors='coerce')

try:
    print("Lendo o arquivo 'dados_limpos_pcj.csv' com o decodificador UTF-8-SIG...")
    
    df_temp = pd.read_csv('dados_limpos_pcj.csv', sep=';', encoding='utf-8-sig', header=0)
    
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

    df_dados.rename(columns={
        'Município': 'Municipio',
        'População Total Residente ': 'pop_total',
        'População Urbana Residente': 'pop_urbana',
        'População Rural Residente ': 'pop_rural'
    }, inplace=True)
    
    # A linha abaixo precisa estar alinhada
    if 'Municipio' in df_dados.columns:
        df_dados['Municipio'] = df_dados['Municipio'].str.strip()

    # Continue com o restante da lógica...
    # Se houver mais código dentro do try, garanta que esteja indentado corretamente.
    
    # Convertendo colunas numéricas
    colunas_numericas = df_dados.columns[10:]
    for col in colunas_numericas:
        df_dados[col] = to_numeric_br(df_dados[col])

    # Se a conversão falhou, tratar as colunas
    df_dados['perdas_percentual'] = df_dados['perdas_percentual'].astype(str).str.replace(',', '.', regex=False)
    df_dados['perdas_percentual'] = pd.to_numeric(df_dados['perdas_percentual'], errors='coerce')
    
except FileNotFoundError:
    print("Erro: O arquivo 'dados_limpos_pcj.csv' não foi encontrado.")
except Exception as e:
    print(f"Ocorreu um erro ao carregar o arquivo CSV: {e}")


@app.route('/')
def home():
    return open('index.html').read()
    
# ... restante das suas rotas
# Certifique-se de que todas as rotas e funções estejam alinhadas.
