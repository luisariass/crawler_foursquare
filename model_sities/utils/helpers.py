"""
Funciones auxiliares para el scraper
"""
import time
from typing import Dict, Any

def current_timestamp() -> str:
    """Devuelve la fecha y hora actual como string"""
    return time.strftime("%Y-%m-%d %H:%M:%S")

def load_credentials(file_path: str) -> Dict[str, str]:
    """Carga las credenciales desde un archivo"""
    credentials = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    credentials[key.strip()] = value.strip()
        return credentials
    except Exception as e:
        print(f"Error al cargar credenciales: {e}")
        return {}
        
def print_progress(current: int, total: int, prefix: str = "Progreso") -> None:
    """Imprime información de progreso"""
    percentage = (current / total) * 100 if total > 0 else 0
    print(f"{prefix}: {current}/{total} ({percentage:.1f}%)")