import os
import json
import numpy as np
import pandas as pd
from playwright.sync_api import Page
from config.settings import Settings
from utils.helpers import current_timestamp

PROGRESO_PATH = "progreso_reseñas.json"

def guardar_progreso(idx_actual, csv_path, sitios_bloqueados):
    progreso = {
        "csv_path": csv_path,
        "idx_actual": idx_actual,
        "sitios_bloqueados": sitios_bloqueados
    }
    with open(PROGRESO_PATH, "w", encoding="utf-8") as f:
        json.dump(progreso, f, ensure_ascii=False, indent=4)
    print(f"Progreso guardado en {PROGRESO_PATH}")

def cargar_progreso():
    if os.path.exists(PROGRESO_PATH):
        with open(PROGRESO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

class FoursquareReviewsExtractor:
    """
    Extrae reseñas de sitios turísticos usando Playwright, leyendo URLs desde archivos CSV.
    """

    def __init__(self, output_dir="reseñas_sitios"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_reviews_from_csv(self, page: Page, csv_path):
        df = pd.read_csv(csv_path)
        progreso = cargar_progreso()
        start_idx = 0

        if progreso and progreso["csv_path"] == csv_path:
            start_idx = progreso["idx_actual"]
            print(f"Reanudando desde el sitio {start_idx} en {csv_path}")

        for idx, row in enumerate(df.itertuples(), start=0):
            if idx < start_idx:
                continue

            url = getattr(row, "url_sitio", None)
            nombre = getattr(row, "nombre", f"sitio_{idx}")
            municipio = getattr(row, "municipio", "desconocido")
            if not isinstance(url, str) or not url.startswith("http"):
                continue

            print(f"Extrayendo reseñas de: {nombre} ({url})")
            reviews = self._extract_reviews_from_site(page, url, nombre)

            if reviews == "BLOQUEADO":
                print("Bloqueo detectado. Guardando progreso y deteniendo el scraper.")
                guardar_progreso(idx, csv_path, [])
                return  # Detener el scraper aquí

            # --- GUARDAR LAS RESEÑAS EXTRAÍDAS ---
            ciudad_dir = os.path.join(self.output_dir, municipio)
            os.makedirs(ciudad_dir, exist_ok=True)
            nombre_archivo = f"reseñas_sitio_{nombre.replace(' ', '_').replace('/', '_')}.json"
            path_out = os.path.join(ciudad_dir, nombre_archivo)
            with open(path_out, "w", encoding="utf-8") as f_out:
                json.dump(reviews, f_out, ensure_ascii=False, indent=4)
            print(f"  Reseñas guardadas en: {path_out}")

            # --- GUARDAR PROGRESO DESPUÉS DE CADA SITIO ---
            guardar_progreso(idx + 1, csv_path, [])

    def _extract_reviews_from_site(self, page: Page, url: str, nombre_sitio: str):
        page.goto(url)
        page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_MEDIUM_MIN, Settings.WAIT_MEDIUM_MAX)))
        # Detector de bloqueo
        if "Sorry! We're having technical difficulties." in page.content():
            print("Bloqueo detectado. Pausando scraping por 10 minutos...")
            page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_EXTRA_LONG_MIN, Settings.WAIT_EXTRA_LONG_MAX)) * 5)  # 10 minutos aprox
            page.reload()
            page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_MEDIUM_MIN, Settings.WAIT_MEDIUM_MAX)))
            if "Sorry! We're having technical difficulties." in page.content():
                print("El bloqueo persiste tras la pausa.")
                return "BLOQUEADO"

        # Intentar hacer clic en el filtro "Recientes" si existe
        try:
            recientes_btn = page.query_selector('//span[contains(@class, "sortLink") and contains(text(), "Recientes")]')
            if recientes_btn:
                recientes_btn.click()
                page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_SHORT_MIN, Settings.WAIT_SHORT_MAX)))
        except Exception:
            pass

        # Extraer reseñas visibles
        reviews = []
        review_elements = page.query_selector_all('div.tipContents')
        for tip in review_elements:
            contenido = tip.query_selector('div.tipText')
            usuario_tag = tip.query_selector('span.userName a')
            usuario_nombre = usuario_tag.inner_text().strip() if usuario_tag else ""
            perfil_url_usuario = usuario_tag.get_attribute('href') if usuario_tag else ""
            if perfil_url_usuario and perfil_url_usuario.startswith('/'):
                perfil_url_usuario = f"{Settings.BASE_URL}{perfil_url_usuario}"
            fecha = tip.query_selector('span.tipDate')
            review = {
                "usuario": usuario_nombre,
                "contenido": contenido.inner_text().strip() if contenido else "",
                "fecha_reseña": fecha.inner_text().strip() if fecha else "",
                "lugar": nombre_sitio,
                "perfil_url_usuario": perfil_url_usuario,
                "perfil_url": url
            }
            reviews.append(review)
        print(f"  Total reseñas extraídas: {len(reviews)}")
        return reviews