from flask import Flask, render_template, jsonify
import pandas as pd
from unidecode import unidecode

app = Flask(__name__)

CSV_PATH = "dados.csv"  # caminho do seu CSV

# Função para carregar e preparar os dados
def carregar_dados():
    try:
        print("--- INICIANDO CARREGAMENTO DOS DADOS ---")
        # Usa engine='python' para lidar com aspas e vírgulas internas
        df = pd.read_csv(CSV_PATH, sep=',', quotechar='"', engine='python', encoding='utf-8')
        print(f"--- Dados carregados: {df.shape[0]} linhas, {df.shape[1]} colunas ---")
        # Remove acentos do nome dos municípios
        df['Municipio'] = df['Municipio'].apply(lambda x: unidecode(str(x)))
        print("Amostra de municípios:", df['Municipio'].head().tolist())
        return df
    except Exception as e:
        print("ERRO CRÍTICO AO CARREGAR DADOS:", e)
        return pd.DataFrame()

# Carrega os dados ao iniciar o app
df_dados = carregar_dados()

[
  {"Municipio": "Camanducaia", "UF": "MG", "Populacao_Total_Residente_": 15000},
  {"Municipio": "Extrema", "UF": "MG", "Populacao_Total_Residente_": 35000}
]

# Rota principal
@app.route("/")
def index():
    # Apenas renderiza o HTML
    return render_template("index.html")

# Rota para obter a lista de municípios e informações resumidas
@app.route("/municipios")
def municipios():
    if df_dados.empty:
        return jsonify({"error": "Dados não carregados"}), 500
    municipios_info = df_dados[['Municipio', 'UF', 'Populacao_Total_Residente_']].to_dict(orient='records')
    return jsonify(municipios_info)

# Rota para detalhes de um município específico
@app.route("/municipio/<nome>")
def detalhe_municipio(nome):
    if df_dados.empty:
        return jsonify({"error": "Dados não carregados"}), 500
    nome = unidecode(nome.lower())
    df_filtered = df_dados[df_dados['Municipio'].str.lower() == nome]
    if df_filtered.empty:
        return jsonify({"error": "Município não encontrado"}), 404
    return jsonify(df_filtered.to_dict(orient='records'))

if __name__ == "__main__":
    app.run(debug=True)

