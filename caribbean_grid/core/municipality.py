import requests
from config.settings import DATOSGOV_API

def fetch_municipalities(departament):
    """Obtiene municipios de un departamento con los campos necesarios, incluyendo los códigos."""
    params = {
        "$where": f"dpto='{departament}'",
        # Añadimos los códigos de departamento y municipio a la selección
        "$select": "nom_mpio, dpto, longitud, latitud, cod_dpto, cod_mpio",
    }
    try:
        response = requests.get(DATOSGOV_API, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error obteniendo municipios: {e}")
        return []