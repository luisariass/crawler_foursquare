from playwright.sync_api import sync_playwright
from core.user_reviews import UserReviewsExtractor
from config.settings import Settings
from utils.cookies_helper import cargar_cookies_playwright
import argparse

def main():
    parser = argparse.ArgumentParser(description='Extractor de reseñas de usuarios de Foursquare')
    parser.add_argument('--csv', type=str, default=Settings.USERS_CSV,
                        help='Ruta al archivo CSV con información de usuarios')
    parser.add_argument('--headless', action='store_true',
                        help='Ejecutar en modo headless (sin interfaz gráfica)')
    args = parser.parse_args()
    headless = args.headless if args.headless else Settings.HEADLESS

    extractor = UserReviewsExtractor()
    with sync_playwright() as p:
        browser = getattr(p, Settings.BROWSER_TYPE).launch(headless=headless)
        page = browser.new_page()
        if not cargar_cookies_playwright(page, Settings.COOKIES_PATH):
            print("Error cargando cookies")
            return
        extractor.extract_reviews_from_csv(page, args.csv)
        browser.close()

if __name__ == "__main__":
    main()