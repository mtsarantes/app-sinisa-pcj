import pandas as pd
from flask import Flask, jsonify, render_template
from flask_cors import CORS
import numpy as np
import io

app = Flask(__name__)
CORS(app)

app.config['JSON_AS_ASCII'] = False

df_dados = pd.DataFrame()

try:
    print("--- INICIANDO CARREGAMENTO DOS DADOS ---")
    print("PASSO 1: Lendo o arquivo 'dados_limpos_pcj.csv'...")
    df_temp = pd.read_csv(
        'dados_limpos_pcj.csv', 
        sep=',', 
        encoding='utf-8-sig', 
        header=0,
        skiprows=[1, 2],
        engine='python'
    )
    
    print("PASSO 2: Renomeando colunas para nomes padronizados (com correspondência exata)...")
    
    # A CORREÇÃO FINAL E MAIS IMPORTANTE ESTÁ AQUI:
    # Ajustando os nomes para bater exatamente com o cabeçalho do seu CSV, incluindo espaços no final.
    df_temp.rename(columns={
        'Municipio': 'Municipio',
        'Populacao Total Residente ': 'pop_total', #<-- Nome corrigido com espaço
        'Populacao Urbana Residente': 'pop_urbana',
        'Populacao Rural Residente ': 'pop_rural', #<-- Nome corrigido com espaço
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

    if 'Municipio' in df_temp.columns:
        print("PASSO 3: Processando e convertendo tipos de dados...")
        df_temp['Municipio'] = df_temp['Municipio'].str.strip()
        
        cols_to_convert = [
            'pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 'vol_consumido',
            'vol_micromedido', 'perdas_percentual', 'perdas_lineares', 'perdas_por_ligacao',
            'incidencia_setorizadas', 'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025'
        ]
        
        for col in cols_to_convert:
            if col in df_temp.columns:
                df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce')
        
        # Atribui o dataframe processado à variável global
        df_dados = df_temp
        
        print("\n--- DIAGNÓSTICO DO DATAFRAME CARREGADO ---")
        print(f"Formato do DataFrame (Linhas, Colunas): {df_dados.shape}")
        print("Amostra dos dados (5 primeiras linhas):")
        print(df_dados[['Municipio', 'pop_total', 'perdas_percentual']].head())
        print("--- FIM DO DIAGNÓSTICO ---\n")

        print("SUCESSO: Dados carregados e prontos para uso!")
    else:
        print("ERRO: A coluna 'Municipio' não foi encontrada. Verifique o dicionário 'rename'.")
        print("Colunas disponíveis após tentativa de renomear:", df_temp.columns.tolist())
        raise Exception("Falha ao processar as colunas.")
        
except Exception as e:
    print(f"\nERRO CRÍTICO DURANTE O CARREGAMENTO DOS DADOS: {e}")

# --- Rotas da API ---

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/municipios')
def get_municipios():
    print(f"\n[API /api/municipios] Recebida requisição. Status do df_dados: Vazio? {df_dados.empty}")
    if df_dados.empty: return jsonify({"erro": "Dados não carregados no servidor."}), 500
    
    municipios_validos = df_dados.dropna(subset=['Municipio'])
    municipios_validos = municipios_validos[municipios_validos['Municipio'].apply(isinstance, args=(str,))]
    lista_municipios = sorted(municipios_validos['Municipio'].unique().tolist())
    print(f"  - Retornando {len(lista_municipios)} municípios.")
    return jsonify(lista_municipios)

@app.route('/api/ranking/perdas')
def ranking_perdas():
    print(f"\n[API /api/ranking/perdas] Recebida requisição. Status do df_dados: Vazio? {df_dados.empty}")
    if df_dados.empty: return jsonify({"erro": "Dados não carregados no servidor."}), 500
    
    ranking_df = df_dados.dropna(subset=['perdas_percentual', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_percentual', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_percentual']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/ranking/perdas_por_ligacao')
def ranking_perdas_por_ligacao():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados no servidor."}), 500
    ranking_df = df_dados.dropna(subset=['perdas_por_ligacao', 'Municipio'])
    ranking_df = ranking_df.sort_values(by='perdas_por_ligacao', ascending=True)
    ranking_df = ranking_df[['Municipio', 'perdas_por_ligacao']]
    ranking_df.insert(0, 'Posicao', range(1, 1 + len(ranking_df)))
    return jsonify(ranking_df.to_dict(orient='records'))

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    print(f"\n[API /api/municipio/{nome_municipio}] Recebida requisição.")
    if df_dados.empty: return jsonify({"erro": "Dados não carregados no servidor."}), 500
    
    df_filtrado = df_dados[df_dados['Municipio'].notna() & df_dados['Municipio'].apply(isinstance, args=(str,))]
    municipio_encontrado = df_filtrado[df_filtrado['Municipio'].str.lower() == nome_municipio.lower()]
    
    if municipio_encontrado.empty:
        return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].replace({np.nan: 'N/D'}).to_dict()
        return jsonify(dados_formatados)
