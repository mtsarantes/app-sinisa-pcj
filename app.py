import os
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template
from flask_cors import CORS

# --- Configuração do Flask ---
app = Flask(__name__)
CORS(app)
app.config['JSON_AS_ASCII'] = False

# --- Variável global para armazenar os dados ---
df_dados = pd.DataFrame()

# --- Função de carregamento do CSV ---
def carregar_dados():
    global df_dados
    try:
        print("--- INICIANDO CARREGAMENTO DOS DADOS ---")
        # Caminho absoluto do CSV
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(BASE_DIR, 'dados_limpos_pcj.csv')

        # Leitura do CSV
        df_temp = pd.read_csv(
            csv_path,
            sep=',',
            encoding='utf-8-sig',
            skiprows=[1, 2],
            engine='python'
        )

        # Remove espaços extras nas colunas
        df_temp.columns = df_temp.columns.str.strip()

        # Renomeia colunas importantes
        df_temp.rename(columns={
            'Municipio': 'Municipio',
            'Populacao Total Residente': 'pop_total',
            'Populacao Urbana Residente': 'pop_urbana',
            'Populacao Rural Residente': 'pop_rural',
            'Volume de agua produzido': 'vol_produzido',
            'Volume de agua consumido': 'vol_consumido',
            'Volume de agua micromedido': 'vol_micromedido',
            'Perdas totais de agua na distribuicao': 'perdas_percentual',
            'Perdas totais lineares de agua na rede de distribuicao': 'perdas_lineares',
            'Perdas totais de agua por ligacao': 'perdas_por_ligacao',
            'Incidencia de ligacoes de agua setorizadas': 'incidencia_setorizadas',
            'Volume de perdas aparentes de agua': 'vol_perdas_aparentes',
            'Volume de perdas reais de agua': 'vol_perdas_reais',
            'Meta 2025': 'Meta_2025'
        }, inplace=True)

        # Converte colunas numéricas
        cols_to_convert = [
            'pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 'vol_consumido',
            'vol_micromedido', 'perdas_percentual', 'perdas_lineares', 'perdas_por_ligacao',
            'incidencia_setorizadas', 'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025'
        ]
        for col in cols_to_convert:
            if col in df_temp.columns:
                df_temp[col] = pd.to_numeric(df_temp[col].astype(str).str.replace(',', '.'), errors='coerce')

        # Calcula percentuais de população urbana e rural
        df_temp['pct_pop_urbana'] = (df_temp['pop_urbana'] / df_temp['pop_total']) * 100
        df_temp['pct_pop_rural'] = (df_temp['pop_rural'] / df_temp['pop_total']) * 100

        # Remove espaços dos nomes de municípios
        df_temp['Municipio'] = df_temp['Municipio'].str.strip()

        # Atribui ao dataframe global
        df_dados = df_temp

        print(f"--- Dados carregados: {df_dados.shape[0]} linhas, {df_dados.shape[1]} colunas ---")
        print("Amostra de municípios:", df_dados['Municipio'].head().tolist())

    except Exception as e:
        print(f"ERRO CRÍTICO AO CARREGAR DADOS: {e}")

# --- Carrega os dados ao iniciar o app ---
carregar_dados()

# --- Rotas do Flask ---
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados no servidor."}), 500

    municipios_validos = df_dados[df_dados['Municipio'].apply(lambda x: isinstance(x, str))]
    lista_municipios = sorted(municipios_validos['Municipio'].unique().tolist())
    return jsonify(lista_municipios)

@app.route('/api/ranking/perdas')
def ranking_perdas():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados no servidor."}), 500

    ranking_df = df_dados.dropna(subset=['perdas_percentual', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_percentual', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_percentual']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados no servidor."}), 500

    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_por_ligacao', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_por_ligacao']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty:
        return jsonify({"erro": "Dados não carregados no servidor."}), 500

    df_filtrado = df_dados[df_dados['Municipio'].notna() & df_dados['Municipio'].apply(lambda x: isinstance(x, str))]
    municipio_encontrado = df_filtrado[df_filtrado['Municipio'].str.lower() == nome_municipio.lower()]

    if municipio_encontrado.empty:
        return jsonify({"erro": "Município não encontrado."}), 404

    # Substitui NaN por 'N/D' para o front-end
    dados_formatados = municipio_encontrado.iloc[0].replace({np.nan: 'N/D'}).to_dict()
    return jsonify(dados_formatados)

# --- Rodar o app localmente (para testes) ---
if __name__ == "__main__":
    app.run(debug=True)
