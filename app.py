import pandas as pd
from flask import Flask, jsonify, render_template
from flask_cors import CORS
import numpy as np

app = Flask(__name__)
CORS(app)

app.config['JSON_AS_ASCII'] = False

df_dados = pd.DataFrame()

try:
    print("PASSO 1: Lendo o arquivo 'dados_limpos_pcj.csv'...")
    df_temp = pd.read_csv(
        'dados_limpos_pcj.csv', 
        sep=',', 
        encoding='utf-8-sig', 
        header=0,
        skiprows=[1, 2],
        engine='python'
    )
    
    print("PASSO 2: Limpando espaços em branco dos nomes das colunas...")
    df_temp.columns = df_temp.columns.str.strip()

    print("PASSO 3: Renomeando colunas para nomes padronizados...")
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

    if 'Municipio' in df_temp.columns:
        print("PASSO 4: Processando e convertendo os dados...")
        df_temp['Municipio'] = df_temp['Municipio'].str.strip()
        
        cols_to_convert = [
            'pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 'vol_consumido',
            'vol_micromedido', 'perdas_percentual', 'perdas_lineares', 'perdas_por_ligacao',
            'incidencia_setorizadas', 'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025'
        ]
        
        for col in cols_to_convert:
            if col in df_temp.columns:
                df_temp[col] = pd.to_numeric(df_temp[col], errors='coerce')
        
        df_temp['pct_pop_urbana'] = (df_temp['pop_urbana'] / df_temp['pop_total'] * 100).fillna(0)
        df_temp['pct_pop_rural'] = (df_temp['pop_rural'] / df_temp['pop_total'] * 100).fillna(0)
        
        df_dados = df_temp
        
        print("SUCESSO: Dados carregados e prontos para uso!")
    else:
        print("COLUNAS ENCONTRADAS APÓS LIMPEZA:", df_temp.columns.tolist())
        raise Exception("A coluna 'Municipio' não foi encontrada após a renomeação.")
        
except Exception as e:
    print(f"\nERRO CRÍTICO DURANTE O CARREGAMENTO DOS DADOS: {e}")
    print("A aplicação iniciará, mas o DataFrame de dados estará vazio.")

# --- As rotas da API (backend) e da página (frontend) continuam iguais ---

@app.route('/')
def home():
    return render_template('index.html')

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

# A CORREÇÃO ESTÁ NESTA FUNÇÃO ABAIXO
@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    
    # Filtra o DataFrame para trabalhar apenas com linhas onde 'Municipio' é um texto válido (ignora nulos/vazios)
    df_filtrado = df_dados[df_dados['Municipio'].notna() & df_dados['Municipio'].apply(isinstance, args=(str,))]
    
    # Realiza a busca case-insensitive no DataFrame já filtrado e seguro
    municipio_encontrado = df_filtrado[df_filtrado['Municipio'].str.lower() == nome_municipio.lower()]
    
    if municipio_encontrado.empty: return jsonify({"erro": "Município não encontrado."}), 404
    else:
        dados_formatados = municipio_encontrado.iloc[0].replace({np.nan: 'N/D'}).to_dict()
        return jsonify(dados_formatados)

@app.route('/api/municipios')
def get_municipios():
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    municipios_validos = df_dados[df_dados['Municipio'].apply(lambda x: isinstance(x, str))]['Municipio']
    lista_municipios = sorted(municipios_validos.dropna().unique().tolist())
    return jsonify(lista_municipios)
