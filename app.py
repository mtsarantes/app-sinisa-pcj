import pandas as pd
from flask import Flask, jsonify, Response
from flask_caching import Cache
import orjson # Biblioteca de JSON de alta performance

# --- Configuração da Aplicação e Cache ---
# Configuração para um cache simples em memória, ideal para ambientes de servidor único.
# O timeout define que cada item no cache expira após 1 hora (3600 segundos).
config = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 3600
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
    # 1. Carregamento estratégico com limpeza proativa (Secção 1.1)
    na_markers =
    # Assumindo que as duas primeiras linhas são metadados. Ajustar se necessário.
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='utf-8',
        na_values=na_markers,
        skiprows= 
    )

    # 2. Sanitização e otimização de memória (Secção 1.2)
    # Identificar colunas que deveriam ser numéricas para conversão
    # Esta lista deve ser expandida com base nas colunas reais do ficheiro
    numeric_cols =
    
    for col in numeric_cols:
        if col in df.columns:
            # Converte para numérico, forçando erros para NaN, e faz downcast
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')

    # Converter colunas de baixa cardinalidade para o tipo 'category' para economizar memória
    categorical_cols = ['Macrorregião', 'UF', 'Natureza Juridica']
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # 3. Preparação para pesquisas rápidas (Secção 2.2)
    # Definir a coluna 'Município' como índice para pesquisas O(1)
    df.set_index('Município', inplace=True)
    
    # Remover colunas que possam ter nomes problemáticos ou que não sejam necessárias
    df.columns = df.columns.str.strip()

    print(">>> SUCESSO: Dados carregados, limpos e otimizados!")
    return df

# Carrega os dados globalmente quando o servidor inicia
# Idealmente, este seria um ficheiro.parquet para um carregamento mais rápido
caminho_arquivo = 'dados_limpos_pcj.csv'
dados_pcj = load_and_prepare_data(caminho_arquivo)

# --- Endpoints da API ---

@app.route('/api/municipio/<string:nome_municipio>')
@cache.cached() # Aplica o cache a este endpoint (Secção 3.2)
def dados_municipio(nome_municipio: str):
    """
    Retorna os dados completos para um município específico.
    Utiliza pesquisa indexada para performance máxima.
    """
    try:
        # 1. Pesquisa O(1) usando.loc no índice (Secção 2.2)
        # O.strip() garante que espaços em branco acidentais no URL não quebrem a pesquisa
        dados = dados_pcj.loc[nome_municipio.strip()]

        # 2. Substituir NaN por None para compatibilidade JSON
        # O fillna(None) é mais idiomático que um loop manual
        dados_limpos = dados.where(pd.notna(dados), None)

        # 3. Serialização eficiente para JSON (Secção 2.3)
        # Converter a Series para um dicionário e usar orjson para performance
        data_dict = dados_limpos.to_dict()
        
        # Usar orjson para uma serialização mais rápida que o jsonify padrão
        return Response(orjson.dumps(data_dict), mimetype='application/json')

    except KeyError:
        # Retorna um erro 404 se o município não for encontrado no índice
        return jsonify({"erro": f"Município '{nome_municipio}' não encontrado."}), 404
    except Exception as e:
        # Captura outros erros inesperados
        return jsonify({"erro": "Ocorreu um erro interno no servidor.", "detalhes": str(e)}), 500

@app.route('/api/rankings/perdas')
@cache.cached()
def ranking_perdas():
    """
    Retorna um ranking dos municípios com base no índice de perdas.
    """
    try:
        # Assegurar que a coluna de perdas existe e é numérica
        coluna_perdas = 'Perdas totais de água na distribuição'
        if coluna_perdas not in dados_pcj.columns:
            return jsonify({"erro": f"Coluna '{coluna_perdas}' não encontrada."}), 500

        # Criar o ranking, removendo valores nulos e ordenando
        ranking = dados_pcj[[coluna_perdas]].dropna().sort_values(by=coluna_perdas, ascending=False)
        
        # Resetar o índice para que 'Município' se torne uma coluna novamente
        ranking_dict = ranking.reset_index().to_dict(orient='records')
        
        return Response(orjson.dumps(ranking_dict), mimetype='application/json')

    except Exception as e:
        return jsonify({"erro": "Ocorreu um erro ao gerar o ranking.", "detalhes": str(e)}), 500

# O bloco a seguir é apenas para execução local (desenvolvimento).
# Em produção, o Gunicorn irá importar e executar o objeto 'app'.
if __name__ == '__main__':
    app.run(debug=True)
