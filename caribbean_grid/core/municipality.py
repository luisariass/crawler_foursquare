import requests
from config.settings import API_URL

def fetch_official_municipalities(department: str):
    params = {
        "$query": f"SELECT cod_dpto, dpto, cod_mpio, nom_mpio, tipo_municipio, longitud, latitud WHERE dpto = '{department}'"
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()
    return response.json()