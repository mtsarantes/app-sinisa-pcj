import pandas as pd
from flask import Flask, Response
from flask_caching import Cache
import orjson # Biblioteca de JSON de alta performance

# --- Configuração da Aplicação e Cache ---
config = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 3600 # Cache de 1 hora
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
    # CORRIGIDO: Lista de valores a serem tratados como nulos, agora preenchida.
    na_markers =
    
    # Lê o cabeçalho da primeira linha (índice 0) e ignora as duas linhas seguintes.
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='utf-8',
        na_values=na_markers,
        header=0, # O cabeçalho está na primeira linha
        skiprows=[1, 2] # Ignora a segunda e terceira linhas
    )

    # Remove espaços em branco dos nomes das colunas
    df.columns = df.columns.str.strip()

    # CORRIGIDO: Lista de colunas numéricas a serem otimizadas, agora preenchida.
    numeric_cols =
    
    for col in numeric_cols:
        if col in df.columns:
            # Converte para numérico, tratando vírgulas e forçando erros para NaN.
            if df[col].dtype == 'object':
                 df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')

    # Converter colunas de baixa cardinalidade para o tipo 'category' para economizar memória
    categorical_cols = ['Macrorregião', 'UF', 'Natureza Juridica']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Preparação para pesquisas rápidas: Definir a coluna 'Município' como índice.
    df.dropna(subset=['Município'], inplace=True)
    df['Município'] = df['Município'].str.strip()
    df.set_index('Município', inplace=True)
    
    print(">>> SUCESSO: Dados carregados, limpos e otimizados!")
    return df

# --- Inicialização dos Dados ---
try:
    caminho_arquivo = 'dados_limpos_pcj.csv'
    dados_pcj = load_and_prepare_data(caminho_arquivo)
    DATA_LOAD_ERROR = None
except Exception as e:
    DATA_LOAD_ERROR = f"Erro crítico na inicialização: {str(e)}"
    dados_pcj = pd.DataFrame() # Garante que a variável existe, mesmo em caso de erro.
    print(f"--- ERRO AO CARREGAR OS DADOS: {DATA_LOAD_ERROR} ---")


# --- Endpoints da API ---

@app.route('/api/municipio/<string:nome_municipio>')
@cache.cached() # Aplica o cache a este endpoint
def dados_municipio(nome_municipio: str):
    """
    Retorna os dados completos para um município específico usando pesquisa indexada.
    """
    if DATA_LOAD_ERROR:
        return Response(orjson.dumps({"erro": DATA_LOAD_ERROR}), status=500, mimetype='application/json')

    try:
        # Pesquisa O(1) usando.loc no índice.
        dados = dados_pcj.loc[nome_municipio.strip()]
        
        # Substituir NaN por None para compatibilidade JSON
        dados_limpos = dados.where(pd.notna(dados), None)
        
        # Converter a Series para um dicionário e usar orjson para performance
        data_dict = dados_limpos.to_dict()
        
        return Response(orjson.dumps(data_dict), mimetype='application/json')

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
            return Response(orjson.dumps({"erro": f"Coluna '{coluna_perdas}' não encontrada."}), status=500, mimetype='application/json')

        ranking = dados_pcj[[coluna_perdas]].dropna().sort_values(by=coluna_perdas, ascending=True)
        ranking_dict = ranking.reset_index().to_dict(orient='records')
        
        return Response(orjson.dumps(ranking_dict), mimetype='application/json')

    except Exception as e:
        return Response(orjson.dumps({"erro": "Ocorreu um erro ao gerar o ranking.", "detalhes": str(e)}), status=500, mimetype='application/json')

# --- Fim dos Endpoints ---

# Este bloco não é executado no Render, que usa Gunicorn.
if __name__ == '__main__':
    app.run(debug=True)
