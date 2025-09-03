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
    
    # A CORREÇÃO FINAL: Lendo o arquivo como 'utf-8-sig'
    df_temp = pd.read_csv('dados_limpos_pcj.csv', sep=';', encoding='utf-8-sig', header=0)
    
    # Pulando as 2 linhas extras (unidades, códigos)
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

     if 'Municipio' in df_dados.columns:
        df_dados['Municipio'] = df_dados['Municipio'].str.strip()
        
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

# ... (O resto do código permanece o mesmo) ...
@app.route('/api/ranking/perdas')
def ranking_perdas():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    ranking_df = df_dados.dropna(subset=['perdas_percentual', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_percentual', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_percentual']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_por_ligacao', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_por_ligacao']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    municipio_encontrado = df_dados[df_dados['Municipio'].str.lower() == nome_municipio.lower()]
    if municipio_encontrado.empty: return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].fillna('N/D').to_dict()
        return jsonify(dados_formatados)

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    lista_municipios = sorted(df_dados['Municipio'].dropna().unique().tolist())
    return jsonify(lista_municipios)

if __name__ == '__main__':
    if not df_dados.empty:
        print("\nIniciando o servidor...")
        app.run(debug=True)
    else:
        print("\nServidor não iniciado devido a erro no carregamento dos dados.")

