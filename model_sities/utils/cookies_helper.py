import json
import os
from playwright.sync_api import sync_playwright
from ..config.settings import Settings


def save_cookies(page, cookies_path=Settings.COOKIES_JSON):
    cookies = page.context.cookies()
    with open(cookies_path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=4)
    print(f"Cookies guardadas en {cookies_path}")

def load_cookies(page, cookies_path=Settings.COOKIES_JSON):
    if not os.path.exists(cookies_path):
        print(f"No se encontró el archivo de cookies: {cookies_path}")
        return False
    with open(cookies_path, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    page.context.add_cookies(cookies)
    print("Cookies cargadas correctamente.")
    return True

def main():
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()
        page.goto("https://es.foursquare.com/login")
        print("Inicia sesión manualmente y completa el 2FA si es necesario.")
        input("Presiona Enter aquí cuando hayas terminado el login y 2FA...")
        save_cookies(page, Settings.COOKIES_JSON)
        browser.close()

if __name__ == "__main__":
    main()