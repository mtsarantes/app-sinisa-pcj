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

    # Leitura robusta que funcionou na análise
    df_temp = pd.read_csv(caminho_arquivo, sep=';', encoding='ISO-8859-1', header=0)
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

    # DICIONÁRIO DE RENOMEAÇÃO CORRIGIDO COM OS NOMES EXATOS DO SEU CSV
    rename_map = {
        'MunicÃ\xadpio': 'Municipio',
        'Meta 2025': 'Meta_2025',
        'PopulaÃ§Ã£o Total Residente ': 'pop_total',
        'PopulaÃ§Ã£o Urbana Residente': 'pop_urbana',
        'PopulaÃ§Ã£o Rural Residente ': 'pop_rural',
        'Volume de Ã¡gua produzido': 'vol_produzido',
        'Volume de Ã¡gua consumido': 'vol_consumido',
        'Volume de Ã¡gua micromedido': 'vol_micromedido',
        'Perdas totais de Ã¡gua na distribuiÃ§Ã£o': 'perdas_percentual',
        'Perdas totais lineares de Ã¡gua na rede de distribuiÃ§Ã£o': 'perdas_lineares',
        'Perdas totais de Ã¡gua por ligaÃ§Ã£o': 'perdas_por_ligacao',
        'IncidÃªncia de ligaÃ§Ãµes de Ã¡gua setorizadas': 'incidencia_setorizadas',
        'Volume de perdas aparentes de Ã¡gua': 'vol_perdas_aparentes',
        'Volume de perdas reais de Ã¡gua': 'vol_perdas_reais'
    }
    df_dados.rename(columns=rename_map, inplace=True)

    if 'Municipio' in df_dados.columns:
        df_dados['Municipio'] = df_dados['Municipio'].str.strip()
        
        cols_to_convert = ['pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 'vol_consumido', 'vol_micromedido', 'perdas_percentual','perdas_lineares', 'perdas_por_ligacao', 'incidencia_setorizadas', 'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025']
        for col in cols_to_convert:
            if col in df_dados.columns:
                df_dados[col] = to_numeric_br(df_dados[col])
        
        df_dados['pct_pop_urbana'] = (df_dados['pop_urbana'] / df_dados['pop_total'] * 100).fillna(0)
        df_dados['pct_pop_rural'] = (df_dados['pop_rural'] / df_dados['pop_total'] * 100).fillna(0)
        
        print(">>> SUCESSO: Dados carregados e processados!")
    else:
        raise Exception("A coluna 'Municipio' não foi encontrada. A renomeação falhou.")
        
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
