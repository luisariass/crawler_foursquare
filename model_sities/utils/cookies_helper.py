import json
import os

def guardar_cookies_playwright(page, cookies_path="cookies_foursquare.json"):
    cookies = page.context.cookies()
    with open(cookies_path, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=4)
    print(f"Cookies guardadas en {cookies_path}")

def cargar_cookies_playwright(page, cookies_path="cookies_foursquare.json"):
    if not os.path.exists(cookies_path):
        print(f"No se encontró el archivo de cookies: {cookies_path}")
        return False
    with open(cookies_path, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    page.context.add_cookies(cookies)
    print("Cookies cargadas correctamente.")
    return True