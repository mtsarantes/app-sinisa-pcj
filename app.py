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
    return pd.to_numeric(series.astype(str).str.replace('.', '', regex=False).str.replace(',', '.'), errors='coerce')

# --- DADOS DAS METAS 2025 ---
metas_data = {
    'Municipio_Meta': ['Águas de São Pedro', 'Americana', 'Amparo', 'Analândia', 'Artur Nogueira', 'Atibaia', 'Bom Jesus dos Perdões', 'Bragança Paulista', 'Cabreúva', 'Camanducaia', 'Campinas', 'Campo Limpo Paulista', 'Capivari', 'Charqueada', 'Cordeirópolis', 'Corumbataí', 'Cosmópolis', 'Dois Córregos', 'Elias Fausto', 'Extrema', 'Holambra', 'Hortolândia', 'Indaiatuba', 'Ipeúna', 'Iracemápolis', 'Itapeva', 'Itatiba', 'Itirapina', 'Itupeva', 'Jaguariúna', 'Jarinu', 'Joanópolis', 'Jundiaí', 'Limeira', 'Louveira', 'Mairiporã', 'Mogi Mirim', 'Mombuca', 'Monte Alegre do Sul', 'Monte Mor', 'Morungaba', 'Nazaré Paulista', 'Nova Odessa', 'Paulínia', 'Pedra Bela', 'Pedreira', 'Pinhalzinho', 'Piracaia', 'Piracicaba', 'Rafard', 'Rio Claro', 'Rio das Pedras', 'Saltinho', 'Salto', "Santa Bárbara d'Oeste", 'Santa Gertrudes', 'Santa Maria da Serra', 'Santo Antônio de Posse', 'São Pedro', 'Sapucaí-Mirim', 'Socorro', 'Sumaré', 'Toledo', 'Torrinha', 'Tuiuti', 'Valinhos', 'Vargem', 'Várzea Paulista', 'Vinhedo'],
    'Meta_2025': [30, 26, 25, 50, 25, 25, 23, 27, 31, 28, 22, 39, 29, 36, 20, 17, 25, 45, 23, 32, 30, 28, 25, 26, 34, 30, 37, 25, 25, 42, 39, 17, 38, 16, 27, 34, 46, 19, 25, 30, 32, 28, 25, 30, 11, 25, 28, 29, 31, 24, 39, 25, 58, 44, 25, 20, 19, 12, 60, 17, 23, 48, 30, 37, 53, 25, 30, 35, 25]
}
df_metas = pd.DataFrame(metas_data)

try:
    caminho_arquivo = "Copilado SINISA 2023 - PCJ.xlsx - Gestão Técnica.csv"
    print(f"Lendo o arquivo original: {caminho_arquivo}")

    df_temp = pd.read_csv(caminho_arquivo, sep=',', encoding='latin-1', header=7) # Lendo o CSV original
    df_dados = df_temp.iloc[2:].reset_index(drop=True)

    # Renomeando as colunas para nomes simples
    rename_map = {
        'Município': 'Municipio', 'Meta 2025': 'Meta_2025',
        'População Total Residente ': 'pop_total', 'População Urbana Residente': 'pop_urbana',
        'População Rural Residente ': 'pop_rural', 'Volume de água produzido': 'vol_produzido',
        'Volume de água consumido': 'vol_consumido', 'Volume de água micromedido': 'vol_micromedido',
        'Perdas totais de água na distribuição': 'perdas_percentual',
        'Perdas totais lineares de água na rede de distribuição': 'perdas_lineares',
        'Perdas totais de água por ligação': 'perdas_por_ligacao',
        'Incidência de ligações de água setorizadas': 'incidencia_setorizadas',
        'Volume de perdas aparentes de água': 'vol_perdas_aparentes',
        'Volume de perdas reais de água': 'vol_perdas_reais'
    }
    df_dados.rename(columns=rename_map, inplace=True)
    df_dados.rename(columns=lambda c: c.strip(), inplace=True) # Remove espaços extra dos nomes das colunas

    if 'Municipio' in df_dados.columns:
        df_dados['Municipio'] = df_dados['Municipio'].str.strip()
        df_dados['Municipio_Meta'] = df_dados['Municipio'].str.replace('Santa Bárbara D Oeste', "Santa Bárbara d'Oeste")
        
        df_dados = pd.merge(df_dados, df_metas, left_on='Municipio_Meta', right_on='Municipio_Meta', how='left')
        
        cols_to_convert = ['pop_total', 'pop_urbana', 'pop_rural', 'vol_produzido', 'vol_consumido', 'vol_micromedido', 'perdas_percentual','perdas_lineares', 'perdas_por_ligacao', 'incidencia_setorizadas', 'vol_perdas_aparentes', 'vol_perdas_reais', 'Meta_2025']
        for col in cols_to_convert:
            if col in df_dados.columns:
                df_dados[col] = to_numeric_br(df_dados[col])
        
        df_dados['pct_pop_urbana'] = (df_dados['pop_urbana'] / df_dados['pop_total'] * 100).fillna(0)
        df_dados['pct_pop_rural'] = (df_dados['pop_rural'] / df_dados['pop_total'] * 100).fillna(0)
        
        print(">>> SUCESSO: Dados carregados e processados!")
    else:
        raise Exception("A coluna 'Municipio' não foi encontrada.")
        
except Exception as e:
    DATA_LOAD_ERROR = str(e)
    print(f"\n--- ERRO AO CARREGAR OS DADOS: {e} ---")

# --- Rotas da API ---
# (O código das rotas abaixo permanece o mesmo)

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
