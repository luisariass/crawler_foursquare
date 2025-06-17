import requests
from config.settings import DATOSGOV_API

def fetch_municipalities(departament):
    """Obtiene municipios de un departamento con solo los campos necesarios."""
    params = {
        "$where": f"dpto='{departament}'",
        "$select": "nom_mpio, dpto", # Solo campos esenciales
    }
    try:
        response = requests.get(DATOSGOV_API, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error obteniendo municipios: {e}")
        return []