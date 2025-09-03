from flask import Flask, render_template, jsonify 
import pandas as pd
from unidecode import unidecode
import os

app = Flask(__name__)

CSV_PATH = "dados_limpos_pcj.csv"  # caminho do seu CSV

def carregar_dados():
    try:
        print("--- INICIANDO CARREGAMENTO DOS DADOS ---")
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"Arquivo {CSV_PATH} não encontrado.")
        
        # Tenta ler CSV com separador ; ou , e ignora linhas problemáticas
        df = pd.read_csv(CSV_PATH, sep=None, engine='python', encoding='utf-8', error_bad_lines=False)
        print(f"--- Dados carregados: {df.shape[0]} linhas, {df.shape[1]} colunas ---")
        
        # Padroniza nomes de colunas
        df.columns = [unidecode(c.strip()) for c in df.columns]
        
        # Verifica se as colunas essenciais existem
        for col in ['Municipio', 'UF', 'Populacao_Total_Residente_']:
            if col not in df.columns:
                raise KeyError(f"Coluna {col} não encontrada no CSV.")
        
        # Remove acentos do nome dos municípios
        df['Municipio'] = df['Municipio'].apply(lambda x: unidecode(str(x).strip()))
        print("Amostra de municípios:", df['Municipio'].head().tolist())
        return df
    except Exception as e:
        print("ERRO CRÍTICO AO CARREGAR DADOS:", e)
        return pd.DataFrame()

df_dados = carregar_dados()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/municipios")
def municipios():
    if df_dados.empty:
        return jsonify({"error": "Dados não carregados"}), 500
    municipios_info = df_dados[['Municipio', 'UF', 'Populacao_Total_Residente_']].to_dict(orient='records')
    return jsonify(municipios_info)

@app.route("/municipio/<nome>")
def detalhe_municipio(nome):
    if df_dados.empty:
        return jsonify({"error": "Dados não carregados"}), 500
    nome = unidecode(nome.lower())
    df_filtered = df_dados[df_dados['Municipio'].str.lower() == nome]
    if df_filtered.empty:
        return jsonify({"error": "Município não encontrado"}), 404
    return jsonify(df_filtered.to_dict(orient='records'))

if __name__ == "__main__":
    app.run(debug=True)

