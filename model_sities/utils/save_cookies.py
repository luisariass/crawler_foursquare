from playwright.sync_api import sync_playwright
from .cookies_helper import guardar_cookies_playwright

def main():
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()
        page.goto("https://es.foursquare.com/login")
        print("Inicia sesión manualmente y completa el 2FA si es necesario.")
        input("Presiona Enter aquí cuando hayas terminado el login y 2FA...")
        guardar_cookies_playwright(page)
        browser.close()

if __name__ == "__main__":
    main()