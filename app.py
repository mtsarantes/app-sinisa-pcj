import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
import numpy as np

# --- Configuração da Aplicação ---
app = Flask(__name__, static_folder='.', static_url_path='')
CORS(app) 
app.config['JSON_AS_ASCII'] = False

df_dados = pd.DataFrame()
DATA_LOAD_ERROR = None

def to_numeric_br(series):
    return pd.to_numeric(
        series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.'), 
        errors='coerce'
    )

try:
    caminho_arquivo = "dados_limpos_pcj.csv"
    print(f"Lendo o arquivo: {caminho_arquivo}")

    # Lendo seu arquivo CSV com a codificação que funcionava
    df_temp = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', header=0)
    
    # Pulando as 2 linhas extras (unidades, códigos)
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

    # Renomeando as colunas, incluindo a sua nova coluna de Metas
    df_dados.rename(columns={
        'Município': 'Municipio',
        'População Total Residente ': 'pop_total',
        'População Urbana Residente': 'pop_urbana',
        'População Rural Residente ': 'pop_rural',
        'Volume de água produzido': 'vol_produzido',
        'Volume de água consumido': 'vol_consumido',
        'Volume de água micromedido': 'vol_micromedido',
        'Perdas totais de água na distribuição': 'perdas_percentual',
        'Perdas totais lineares de água na rede de distribuição': 'perdas_lineares',
        'Perdas totais de água por ligação': 'perdas_por_ligacao',
        'Incidência de ligações de água setorizadas': 'incidencia_setorizadas',
        'Volume de perdas aparentes de água': 'vol_perdas_aparentes',
        'Volume de perdas reais de água': 'vol_perdas_reais',
        'Meta 2025': 'Meta_2025'  # <-- ADIÇÃO 1: Renomeando sua nova coluna
    }, inplace=True)

    if 'Municipio' in df_dados.columns:
        df_dados['Municipio'] = df_dados['Municipio'].str.strip()
        
        cols_to_convert = ['pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 
                           'vol_consumido', 'vol_micromedido', 'perdas_percentual',
                           'perdas_lineares', 'perdas_por_ligacao', 'incidencia_setorizadas',
                           'vol_perdas_aparentes', 'vol_perdas_reais',
                           'Meta_2025'] # <-- ADIÇÃO 2: Convertendo a nova coluna para número
        for col in cols_to_convert:
            if col in df_dados.columns:
                df_dados[col] = to_numeric_br(df_dados[col])
        
        df_dados['pct_pop_urbana'] = (df_dados['pop_urbana'] / df_dados['pop_total'] * 100).fillna(0)
        df_dados['pct_pop_rural'] = (df_dados['pop_rural'] / df_dados['pop_total'] * 100).fillna(0)
        
        print("Dados carregados e processados com sucesso!")
    else:
        raise Exception("A coluna 'Municipio' não foi encontrada. Verifique o cabeçalho do CSV.")
        
except Exception as e:
    DATA_LOAD_ERROR = str(e)
    print(f"\n--- ERRO AO CARREGAR OS DADOS: {e} ---")

# --- Rotas da API ---

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty: return jsonify({"erro": "Dados não carregados no servidor."}), 500
    
    municipio_encontrado = df_dados[df_dados['Municipio'].str.lower() == nome_municipio.lower()]
    
    if municipio_encontrado.empty: return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].fillna('N/D').to_dict()
        
        for key, value in dados_formatados.items():
            if isinstance(value, np.generic):
                dados_formatados[key] = None if pd.isna(value) else value.item()

        return jsonify(dados_formatados)

# Outras rotas (rankings, lista de municipios)
@app.route('/api/ranking/perdas')
def ranking_perdas():
    if df_dados.empty: return jsonify([]), 500
    ranking_df = df_dados.dropna(subset=['perdas_percentual', 'Municipio']).sort_values(by='perdas_percentual', ascending=True).reset_index(drop=True)
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df[['Posicao', 'Municipio', 'perdas_percentual']].to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty: return jsonify([]), 500
    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio']).sort_values(by='perdas_por_ligacao', ascending=True).reset_index(drop=True)
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df[['Posicao', 'Municipio', 'perdas_por_ligacao']].to_dict(orient='records'))

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty: return jsonify([]), 500
    lista_municipios = sorted(df_dados['Municipio'].dropna().unique().tolist())
    return jsonify(lista_municipios)

if __name__ == '__main__':
    if not df_dados.empty:
        print("\nIniciando o servidor...")
        app.run(debug=True)
    else:
        print("\nServidor não iniciado devido a erro no carregamento dos dados.")
