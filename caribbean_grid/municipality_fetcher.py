from config.settings import CARIBBEAN_DEPARTMENTS, DEFAULT_OUTPUT_DIR
from core.municipality import fetch_official_municipalities
from utils.helpers import save_municipalities_to_csv

def generate_all_caribbean_departments():
    for dept in CARIBBEAN_DEPARTMENTS:
        municipios = fetch_official_municipalities(dept)
        if municipios:
            save_municipalities_to_csv(municipios, dept, DEFAULT_OUTPUT_DIR)
        else:
            print(f"No se encontraron municipios oficiales para {dept}")

if __name__ == "__main__":
    generate_all_caribbean_departments()