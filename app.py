# -*- coding: utf-8 -*-
import pandas as pd
from flask import Flask, Response
from flask_caching import Cache
import orjson  # Biblioteca de JSON de alta performance

# --- Configuração da Aplicação e Cache ---
config = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 3600  # Cache de 1 hora
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)

# --- Carregamento e Preparação de Dados ---
def load_and_prepare_data(filepath: str) -> pd.DataFrame:
    """
    Carrega, limpa e otimiza o conjunto de dados a partir de um ficheiro CSV.
    Esta função é executada apenas uma vez no arranque da aplicação.
    """
    na_markers = ['*', '***', '-', 'ND', '']

    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='utf-8',
        na_values=na_markers,
        header=0,
        skiprows=[1, 2]
    )

    df.columns = df.columns.str.strip()

    # CORREÇÃO: Lista expandida com todas as colunas numéricas do CSV
    numeric_cols = [
        'População Total Residente',
        'População Urbana Residente',
        'População Rural Residente',
        'População urbana atendida com rede de abastecimento de água',
        'População rural atendida com rede de abastecimento de água',
        'Quantidade de ligações ativas de água',
        'Quantidade de ligações ativas de água micromedidas',
        'Quantidade de ligações inativas de água',
        'Quantidade de ligações totais setorizadas de água',
        'Conexões factíveis de água',
        'Quantidade de economias urbanas ativas de água',
        'Quantidade de economias urbanas residenciais ativas de água',
        'Quantidade de economias ativas de água micromedidas',
        'Quantidade de economias residenciais ativas de água micromedidas',
        'Quantidade de economias inativas de água',
        'Quantidade de economias urbanas residenciais inativas de água',
        'Quantidade de economias factíveis de água',
        'Quantidade de economias rurais ativas de água',
        'Quantidade de economias rurais residenciais ativas de água',
        'Quantidade de economias rurais residenciais inativas de água',
        'Quantidade de domicílios na área de abrangência do Prestador do serviço de abastecimento de água',
        'Volume de água produzido',
        'Volume de água tratada em ETAs',
        'Volume de água importado (bruta e tratada)',
        'Volume de água exportado (bruta e tratada)',
        'Volume de água disponibilizado para o sistema de abastecimento',
        'Volume de água consumido',
        'Volume de água micromedido',
        'Volume de água faturado',
        'Consumo médio per capita de água',
        'Índice de atendimento total de água',
        'Índice de atendimento urbano de água',
        'Índice de hidrometração',
        'Índice de micromedição',
        'Índice de macromedição',
        'Índice de perdas na distribuição',
        'Índice de perdas por ligação',
        'Índice de perdas lineares',
        'Perdas totais de água na distribuição',
        'Índice de setorização',
        'Volume de perdas aparentes',
        'Volume de perdas reais',
        'Índice de atendimento total de esgoto',
        'Índice de tratamento de esgoto',
        'Índice de atendimento total de esgoto referente à água consumida'
    ]

    for col in numeric_cols:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')

    categorical_cols = ['Macrorregião', 'UF', 'Natureza Juridica']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    df.dropna(subset=['Município'], inplace=True)
    df['Município'] = df['Município'].str.strip().str.upper()
    df.set_index('Município', inplace=True)

    print(">>> SUCESSO: Dados carregados, limpos e otimizados!")
    return df

# --- Inicialização dos Dados ---
DATA_LOAD_ERROR = None
dados_pcj = pd.DataFrame()
try:
    caminho_arquivo = 'dados_limpos_pcj.csv'
    dados_pcj = load_and_prepare_data(caminho_arquivo)
except Exception as e:
    DATA_LOAD_ERROR = f"Erro crítico na inicialização: {str(e)}"
    print(f"--- ERRO AO CARREGAR OS DADOS: {DATA_LOAD_ERROR} ---")


# --- Endpoints da API ---

@app.route('/')
def index():
    """Endpoint principal para verificar se a API está no ar."""
    return Response(orjson.dumps({"status": "API online"}), status=200, mimetype='application/json')

@app.route('/api/municipio/<string:nome_municipio>')
@cache.cached()
def dados_municipio(nome_municipio: str):
    """
    Retorna os dados completos para um município específico usando pesquisa indexada.
    """
    if DATA_LOAD_ERROR:
        return Response(orjson.dumps({"erro": DATA_LOAD_ERROR}), status=500, mimetype='application/json')

    try:
        nome_municipio_normalizado = nome_municipio.strip().upper()
        dados = dados_pcj.loc[nome_municipio_normalizado]

        if isinstance(dados, pd.DataFrame):
            dados = dados.iloc[0]

        dados_limpos = dados.where(pd.notna(dados), None)
        data_dict = dados_limpos.to_dict()

        return Response(orjson.dumps(data_dict), status=200, mimetype='application/json')

    except KeyError:
        return Response(orjson.dumps({"erro": f"Município '{nome_municipio}' não encontrado."}), status=404, mimetype='application/json')
    except Exception as e:
        return Response(orjson.dumps({"erro": "Ocorreu um erro interno no servidor.", "detalhes": str(e)}), status=500, mimetype='application/json')

@app.route('/api/rankings/perdas')
@cache.cached()
def ranking_perdas():
    """
    Retorna um ranking dos municípios com base no índice de perdas.
    """
    if DATA_LOAD_ERROR:
        return Response(orjson.dumps({"erro": DATA_LOAD_ERROR}), status=500, mimetype='application/json')

    try:
        coluna_perdas = 'Índice de perdas na distribuição'
        if coluna_perdas not in dados_pcj.columns:
            return Response(orjson.dumps({"erro": f"A coluna '{coluna_perdas}' não foi encontrada nos dados."}), status=500, mimetype='application/json')

        ranking = dados_pcj[[coluna_perdas]].dropna().sort_values(by=coluna_perdas, ascending=True)
        ranking_dict = ranking.reset_index().to_dict(orient='records')

        return Response(orjson.dumps(ranking_dict), status=200, mimetype='application/json')

    except Exception as e:
        return Response(orjson.dumps({"erro": "Ocorreu um erro ao gerar o ranking.", "detalhes": str(e)}), status=500, mimetype='application/json')

# --- Fim dos Endpoints ---

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
