import requests
from datetime import datetime

def download_csv_from_url(logical_date, conjunto, local_csv_path):
    if not isinstance(logical_date, datetime):
        raise ValueError("logical_date deve ser um objeto datetime.")

    year = logical_date.year
    url = f"https://dados.pb.gov.br:443/getcsv?nome={conjunto}_dadospb&exercicio={year}"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(local_csv_path, "wb") as file:
            file.write(response.content)

        print(f"Download concluído: {local_csv_path}")

    except requests.Timeout:
        raise Exception("Erro: Tempo limite da requisição excedido.")
    except requests.RequestException as e:
        raise Exception(f"Erro ao baixar CSV: {e}")
    except IOError as e:
        raise Exception(f"Erro ao salvar arquivo: {e}")
