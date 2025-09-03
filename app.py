import pandas as pd
from flask import Flask, jsonify, render_template
from flask_cors import CORS
import numpy as np

app = Flask(__name__)
CORS(app)

app.config['JSON_AS_ASCII'] = False

df_dados = pd.DataFrame()

try:
    print("PASSO 1: Lendo o arquivo 'dados_limpos_pcj.csv' com o motor Python...")
    
    # A CORREÇÃO FINAL: Adicionado engine='python' para lidar com erros de formatação no CSV, como vírgulas em campos de texto.
    df_temp = pd.read_csv('dados_limpos_pcj.csv', sep=',', encoding='utf-8-sig', header=0, engine='python')
    
    print("PASSO 2: Removendo as 2 linhas de metadados abaixo do cabeçalho...")
    # Esta linha é necessária para pular as linhas de "unidades" e "códigos".
    df_dados_processando = df_temp.iloc[2:].reset_index(drop=True)

    print("PASSO 3: Renomeando as colunas para nomes mais simples...")
    # Renomeando as colunas conforme o cabeçalho do seu arquivo
    df_dados_processando.rename(columns={
        'Municipio': 'Municipio',
        'Populacao Total Residente ': 'pop_total',
        'Populacao Urbana Residente': 'pop_urbana',
        'Populacao Rural Residente ': 'pop_rural',
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

    if 'Municipio' in df_dados_processando.columns:
        print("PASSO 4: Convertendo colunas de texto para números...")
        df_dados_processando['Municipio'] = df_dados_processando['Municipio'].str.strip()
        
        cols_to_convert = [
            'pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 'vol_consumido',
            'vol_micromedido', 'perdas_percentual', 'perdas_lineares', 'perdas_por_ligacao',
            'incidencia_setorizadas', 'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025'
        ]
        
        for col in cols_to_convert:
            if col in df_dados_processando.columns:
                # O errors='coerce' transforma qualquer valor que não seja um número em 'NaN' (Not a Number)
                df_dados_processando[col] = pd.to_numeric(df_dados_processando[col], errors='coerce')
        
        print("PASSO 5: Calculando colunas de percentual...")
        df_dados_processando['pct_pop_urbana'] = (df_dados_processando['pop_urbana'] / df_dados_processando['pop_total'] * 100).fillna(0)
        df_dados_processando['pct_pop_rural'] = (df_dados_processando['pop_rural'] / df_dados_processando['pop_total'] * 100).fillna(0)
        
        df_dados = df_dados_processando
        
        print("SUCESSO: Dados carregados e processados!")
    else:
        raise Exception("A coluna 'Municipio' não foi encontrada. Verifique o cabeçalho do CSV.")
        
except Exception as e:
    print(f"\nERRO INESPERADO DURANTE O CARREGAMENTO: {e}")
    print("O DataFrame 'df_dados' permanecerá vazio. As chamadas de API retornarão erro.")

# --- O restante do código da API permanece o mesmo ---

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

@app.route('/api/municipio/<nome_municipio>')
def dados_municipio(nome_municipio):
    if df_dados.empty: return jsonify({"erro": "Dados não carregados."}), 500
    municipio_encontrado = df_dados[df_dados['Municipio'].str.lower() == nome_municipio.lower()]
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
