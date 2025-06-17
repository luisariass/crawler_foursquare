from config.settings import CARIBBEAN_DEPARTMENTS, DATA_DIR
from core.municipality import fetch_municipalities
from utils.helpers import save_municipalities_to_csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_departments():
    for dept in CARIBBEAN_DEPARTMENTS:
        municipios = fetch_municipalities(dept)
        if municipios:
            save_municipalities_to_csv(municipios, dept, DATA_DIR)
        else:
            logging.warning(f"No se encontraron municipios para {dept}")

if __name__ == "__main__":
    generate_departments()