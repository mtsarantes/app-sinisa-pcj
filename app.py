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
    
    # A CORREÇÃO FINAL: A leitura do arquivo com o header correto e encoding.
    # O header é a primeira linha, por isso header=0.
    df_temp = pd.read_csv('dados_limpos_pcj.csv', sep=';', encoding='utf-8-sig', header=0)
    
    # Pulando as 2 linhas extras (unidades, códigos)
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

    # Renomeando as colunas com os nomes corretos em português
    df_dados.rename(columns={
        'Município': 'Municipio',
        'População Total Residente ': 'pop_total',
        'População Urbana Residente': 'pop_urbana',
        'População Rural Residente ': 'pop_rural',
        'Volume de água produzido': 'volume_produzido',
        'Volume de água faturado nas economias residenciais ativas de água': 'volume_faturado',
        'Volume de perdas reais de água': 'perdas_reais',
        'Perdas totais de água na distribuição': 'perdas_percentual',
        'Perdas totais de água por ligação': 'perdas_por_ligacao',
        'Volume consumido nas economias residenciais ativas de água': 'consumo_residencial',
        'Volume consumido nas economias não residenciais ativas de água': 'consumo_nao_residencial',
        'Consumo total médio per capita de água': 'consumo_per_capita',
        'Quantidade de ligações ativas de água': 'ligacoes_ativas',
        'Volume total de água faturado': 'volume_total_faturado'
    }, inplace=True)

    # Convertendo as colunas numéricas
    colunas_numericas = [
        'pop_total', 'pop_urbana', 'pop_rural', 'volume_produzido', 'volume_faturado',
        'perdas_reais', 'perdas_percentual', 'perdas_por_ligacao', 'consumo_residencial',
        'consumo_nao_residencial', 'consumo_per_capita', 'ligacoes_ativas', 'volume_total_faturado'
    ]
    for col in colunas_numericas:
        df_dados[col] = to_numeric_br(df_dados[col])

    print("Arquivo 'dados_limpos_pcj.csv' carregado e processado com sucesso.")

except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")
    # Cria um DataFrame vazio para evitar erros
    df_dados = pd.DataFrame()

# Suas rotas Flask
@app.route('/')
def home():
    return "API do SINISA-PCJ funcionando! Acesse as rotas /api/bacia/valores, /api/ranking/perdas, etc."

@app.route('/api/bacia/valores')
def bacia_valores():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados."}), 500

    media_pop_total = df_dados['pop_total'].mean()
    media_pop_urbana = df_dados['pop_urbana'].mean()
    media_pop_rural = df_dados['pop_rural'].mean()
    media_volume_produzido = df_dados['volume_produzido'].mean()
    media_volume_faturado = df_dados['volume_total_faturado'].mean()
    media_perdas_percentual = df_dados['perdas_percentual'].mean()

    return jsonify({
        "pop_total": round(media_pop_total, 2),
        "pop_urbana": round(media_pop_urbana, 2),
        "pop_rural": round(media_pop_rural, 2),
        "volume_produzido": round(media_volume_produzido, 2),
        "volume_faturado": round(media_volume_faturado, 2),
        "perdas_percentual": round(media_perdas_percentual, 2)
    })

@app.route('/api/ranking/perdas')
def ranking_perdas():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados."}), 500

    ranking_df = df_dados.dropna(subset=['perdas_percentual', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_percentual', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_percentual']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))

    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados."}), 500

    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_por_ligacao', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_por_ligacao']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))

    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados."}), 500

    municipio_encontrado = df_dados[df_dados['Municipio'].str.lower() == nome_municipio.lower()]

    if municipio_encontrado.empty:
        return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].fillna('N/D').to_dict()
        return jsonify(dados_formatados)

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados."}), 500
    municipios = df_dados['Municipio'].unique().tolist()
    return jsonify(municipios)
    
if __name__ == '__main__':
    app.run(debug=True)
