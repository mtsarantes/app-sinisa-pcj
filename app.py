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
    # CORRIGIDO: Lista de valores a serem tratados como nulos.
    # Adicione outros marcadores conforme necessário para o seu arquivo.
    na_markers = ['*', '***', '-', 'ND', '']

    # Lê o cabeçalho da primeira linha (índice 0) e ignora as duas linhas seguintes.
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='utf-8',
        na_values=na_markers,
        header=0,  # O cabeçalho está na primeira linha
        skiprows=[1, 2]  # Ignora a segunda e terceira linhas
    )

    # Remove espaços em branco dos nomes das colunas
    df.columns = df.columns.str.strip()

    # CORRIGIDO: Lista de colunas numéricas a serem otimizadas.
    # Adicione todas as colunas que devem ser tratadas como números.
    numeric_cols = [
        'Perdas totais de água na distribuição',
        'População total atendida com abastecimento de água',
        'Consumo médio per capita de água',
        'Índice de atendimento total de água',
        'Índice de atendimento total de esgoto'
        # Adicione outras colunas numéricas aqui
    ]

    for col in numeric_cols:
        if col in df.columns:
            # Garante que a coluna é do tipo string antes de usar métodos .str
            if df[col].dtype == 'object':
                # Remove o ponto como separador de milhar e substitui a vírgula por ponto decimal
                df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            # Converte para numérico, tratando erros e otimizando o tipo de dado.
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')

    # Converter colunas de baixa cardinalidade para o tipo 'category' para economizar memória
    categorical_cols = ['Macrorregião', 'UF', 'Natureza Juridica']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Preparação para pesquisas rápidas: Definir a coluna 'Município' como índice.
    df.dropna(subset=['Município'], inplace=True)
    # CORREÇÃO: Normaliza os nomes dos municípios para maiúsculas para busca case-insensitive
    df['Município'] = df['Município'].str.strip().str.upper()
    df.set_index('Município', inplace=True)

    print(">>> SUCESSO: Dados carregados, limpos e otimizados!")
    return df

# --- Inicialização dos Dados ---
DATA_LOAD_ERROR = None
dados_pcj = pd.DataFrame() # Garante que a variável exista, mesmo em caso de erro.
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
@cache.cached()  # Aplica o cache a este endpoint
def dados_municipio(nome_municipio: str):
    """
    Retorna os dados completos para um município específico usando pesquisa indexada.
    """
    if DATA_LOAD_ERROR:
        return Response(orjson.dumps({"erro": DATA_LOAD_ERROR}), status=500, mimetype='application/json')

    try:
        # CORREÇÃO: Normaliza o input para corresponder ao índice (maiúsculas)
        nome_municipio_normalizado = nome_municipio.strip().upper()
        dados = dados_pcj.loc[nome_municipio_normalizado]

        # CORREÇÃO: Se .loc encontrar duplicatas, retorna um DataFrame. Pegamos a primeira linha.
        if isinstance(dados, pd.DataFrame):
            dados = dados.iloc[0]

        # Substituir NaN por None para compatibilidade JSON
        dados_limpos = dados.where(pd.notna(dados), None)

        # Converter a Series para um dicionário e usar orjson para performance
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
        coluna_perdas = 'Perdas totais de água na distribuição'
        if coluna_perdas not in dados_pcj.columns:
            return Response(orjson.dumps({"erro": f"A coluna '{coluna_perdas}' não foi encontrada nos dados."}), status=500, mimetype='application/json')

        # Cria o ranking, remove valores nulos e ordena
        ranking = dados_pcj[[coluna_perdas]].dropna().sort_values(by=coluna_perdas, ascending=True)
        # Converte para dicionário no formato de lista de objetos
        ranking_dict = ranking.reset_index().to_dict(orient='records')

        return Response(orjson.dumps(ranking_dict), status=200, mimetype='application/json')

    except Exception as e:
        return Response(orjson.dumps({"erro": "Ocorreu um erro ao gerar o ranking.", "detalhes": str(e)}), status=500, mimetype='application/json')

# --- Fim dos Endpoints ---

# Este bloco não é executado em produção no Render, que usa um servidor WSGI como o Gunicorn.
# É útil para testes locais.
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

