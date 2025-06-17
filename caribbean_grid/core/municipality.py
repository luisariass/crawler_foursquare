import requests
from config.settings import DATOSGOV_API

def fetch_municipalities(department: str):
    params = {
        "$query": f"SELECT cod_dpto, dpto, cod_mpio, nom_mpio, tipo_municipio, longitud, latitud WHERE dpto = '{department}'"
    }
    response = requests.get(DATOSGOV_API, params=params)
    response.raise_for_status()
    return response.json()