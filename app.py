import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS
import numpy as np

app = Flask(__name__)
CORS(app) 

# Configuração para garantir acentuação correta na página
app.config['JSON_AS_ASCII'] = False

df_dados = pd.DataFrame()

# Função auxiliar para converter números no formato brasileiro
def to_numeric_br(series):
    return pd.to_numeric(series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.'), errors='coerce')

try:
    print("Lendo o arquivo 'dados_limpos_pcj.csv'...")
    
    # Lendo o arquivo com o cabeçalho na primeira linha
    df_temp = pd.read_csv('dados_limpos_pcj.csv', sep=';', encoding='utf-8-sig', header=0)
    
    # Renomeando as colunas com os nomes exatos do seu arquivo CSV
    df_dados = df_temp.rename(columns={
        'Município': 'Municipio',
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
        'Volume de perdas reais de Ã¡gua': 'vol_perdas_reais',
        'Meta 2025': 'Meta_2025'
    }, inplace=False)

    # Pulando as 2 linhas extras (unidades, códigos)
    df_dados = df_dados.iloc[2:].reset_index(drop=True)

    # Limpando e convertendo os dados
    if 'Municipio' in df_dados.columns:
        df_dados['Municipio'] = df_dados['Municipio'].str.strip().str.lower()
        
        cols_to_convert = ['pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 
                           'vol_consumido', 'vol_micromedido', 'perdas_percentual',
                           'perdas_lineares', 'perdas_por_ligacao', 'incidencia_setorizadas',
                           'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025']
        for col in cols_to_convert:
            if col in df_dados.columns:
                df_dados[col] = to_numeric_br(df_dados[col])
        
        df_dados['pct_pop_urbana'] = (df_dados['pop_urbana'] / df_dados['pop_total'] * 100).fillna(0)
        df_dados['pct_pop_rural'] = (df_dados['pop_rural'] / df_dados['pop_total'] * 100).fillna(0)
        
        print("Dados carregados e processados com sucesso!")
    else:
        raise Exception("A coluna 'Municipio' não foi encontrada. Verifique o cabeçalho do CSV.")
        
except Exception as e:
    print(f"\nOcorreu um erro inesperado: {e}")
    df_dados = pd.DataFrame()

@app.route('/')
def home():
    return open('index.html').read()
    
@app.route('/api/bacia/valores')
def bacia_valores():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados."}), 500

    media_perdas_percentual = df_dados['perdas_percentual'].mean()
    media_perdas_por_ligacao = df_dados['perdas_por_ligacao'].mean()
    
    return jsonify({
        "perdas_percentual": round(media_perdas_percentual, 2),
        "perdas_por_ligacao": round(media_perdas_por_ligacao, 2),
    })

@app.route('/api/ranking/perdas')
def ranking_perdas():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    ranking_df = df_dados.dropna(subset=['perdas_percentual', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_percentual', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_percentual']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    
    ranking_df['Municipio'] = ranking_df['Municipio'].str.title()
    
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_por_ligacao', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_por_ligacao']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))

    ranking_df['Municipio'] = ranking_df['Municipio'].str.title()
    
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    
    municipio_encontrado = df_dados[df_dados['Municipio'] == nome_municipio.lower()]
    
    if municipio_encontrado.empty: return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].fillna('N/D').to_dict()
        dados_formatados['Municipio'] = dados_formatados['Municipio'].title() 
        return jsonify(dados_formatados)

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    lista_municipios = sorted(df_dados['Municipio'].str.title().dropna().unique().tolist())
    return jsonify(lista_municipios)
