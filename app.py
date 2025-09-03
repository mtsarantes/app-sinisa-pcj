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
    print("Lendo o arquivo 'dados_limpos_pcj.csv' com o decodificador UTF-8-SIG...")
    
    # Lendo o arquivo e definindo o cabeçalho na primeira linha (índice 0)
    df_temp = pd.read_csv('dados_limpos_pcj.csv', sep=';', encoding='utf-8-sig', header=0)
    
    # Limpando os nomes das colunas
    df_temp.columns = df_temp.columns.str.strip()
    
    # Pulando as 2 linhas extras (unidades, códigos)
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

    # Renomeando as colunas com os nomes corretos em português (após a limpeza)
    df_dados.rename(columns={
        'Município': 'Municipio',
        'População Total Residente': 'pop_total',
        'População Urbana Residente': 'pop_urbana',
        'População Rural Residente': 'pop_rural',
        'Volume de água produzido': 'vol_produzido',
        'Volume de água consumido': 'vol_consumido',
        'Volume de água micromedido': 'vol_micromedido',
        'Perdas totais de água na distribuição': 'perdas_percentual',
        'Perdas totais lineares de água na rede de distribuição': 'perdas_lineares',
        'Perdas totais de água por ligação': 'perdas_por_ligacao',
        'Incidência de ligações de água setorizadas': 'incidencia_setorizadas',
        'Volume de perdas aparentes de água': 'vol_perdas_aparentes',
        'Volume de perdas reais de água': 'vol_perdas_reais',
        'Meta 2025': 'Meta_2025'
    }, inplace=True)
    
    # Limpando a coluna 'Município'
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
    
    # Restaurando a capitalização para exibição
    ranking_df['Municipio'] = ranking_df['Municipio'].str.title()
    
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_por_ligacao', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_por_ligacao']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))

    # Restaurando a capitalização para exibição
    ranking_df['Municipio'] = ranking_df['Municipio'].str.title()
    
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    
    municipio_encontrado = df_dados[df_dados['Municipio'] == nome_municipio.lower()]
    
    if municipio_encontrado.empty: return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].fillna('N/D').to_dict()
        dados_formatados['Municipio'] = dados_formatados['Municipio'].title() # Capitaliza para exibição
        return jsonify(dados_formatados)

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    # Retornando a lista de municípios com a primeira letra maiúscula para a lista suspensa
    lista_municipios = sorted(df_dados['Municipio'].str.title().dropna().unique().tolist())
    return jsonify(lista_municipios)
