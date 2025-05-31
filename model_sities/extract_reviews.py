import os
from playwright.sync_api import sync_playwright
from core.auth import FoursquareAuth
from core.reviews import FoursquareReviewsExtractor
from config.settings import Settings

def main():
    # Rutas a tus archivos CSV
    csv_files = [
        Settings.SITIES_ATLANTICO_CSV,
        Settings.SITIES_BOLIVAR_CSV,
    ]

    extractor = FoursquareReviewsExtractor(output_dir="reseñas_sitios")

    with sync_playwright() as p:
        browser = getattr(p, Settings.BROWSER_TYPE).launch(headless=Settings.HEADLESS)
        page = browser.new_page()
        auth = FoursquareAuth()
        if not auth.login(page):
            print("No se pudo autenticar en Foursquare.")
            return
        for csv_path in csv_files:
            print(f"Procesando archivo: {csv_path}")
            extractor.extract_reviews_from_csv(page, csv_path)
        browser.close()

if __name__ == "__main__":
    main()