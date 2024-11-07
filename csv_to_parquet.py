import pandas as pd
import os

def csv_to_parquet_folder(input_folder, output_folder):
    # Verifica se a pasta de saída existe; se não, cria a pasta
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Percorre todos os arquivos na pasta de entrada
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            input_csv_path = os.path.join(input_folder, filename)
            output_parquet_path = os.path.join(output_folder, filename.replace(".csv", ".parquet"))
            
            # Verifica se o arquivo CSV existe e não está vazio
            if os.stat(input_csv_path).st_size == 0:
                print(f"O arquivo {filename} está vazio e foi ignorado.")
                continue

            # Lê o arquivo CSV
            df = pd.read_csv(input_csv_path)
            
            # Salva o DataFrame em formato Parquet
            df.to_parquet(output_parquet_path, index=False, engine="pyarrow")
            print(f"Arquivo convertido com sucesso para {output_parquet_path}")

# Exemplo de uso
input_folder = "csv"          # Pasta com os arquivos CSV de entrada
output_folder = "parquet"      # Pasta para salvar os arquivos Parquet

csv_to_parquet_folder(input_folder, output_folder)
