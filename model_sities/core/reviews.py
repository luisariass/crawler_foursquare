import os
import json
import numpy as np
import pandas as pd
from playwright.sync_api import Page
from config.settings import Settings
from utils.helpers import current_timestamp

class FoursquareReviewsExtractor:
    """
    Extrae reseñas de sitios turísticos usando Playwright, leyendo URLs desde archivos CSV.
    """

    def __init__(self, output_dir="reseñas_sitios"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def extract_reviews_from_csv(self, page: Page, csv_path):
        """
        Extrae reseñas para cada sitio listado en el archivo CSV.
        """
        df = pd.read_csv(csv_path)
        # Puedes ajustar aquí si quieres más columnas
        for _, row in df.iterrows():
            url = row.get("url_sitio")
            nombre = row.get("nombre", "sitio")
            municipio = row.get("municipio", "desconocido")
            if not isinstance(url, str) or not url.startswith("http"):
                continue
            ciudad_dir = os.path.join(self.output_dir, municipio)
            os.makedirs(ciudad_dir, exist_ok=True)
            nombre_archivo = f"reseñas_sitio_{nombre.replace(' ', '_').replace('/', '_')}.json"
            path_out = os.path.join(ciudad_dir, nombre_archivo)
            print(f"Extrayendo reseñas de: {nombre} ({url})")
            reviews = self._extract_reviews_from_site(page, url, nombre)
            with open(path_out, "w", encoding="utf-8") as f_out:
                json.dump(reviews, f_out, ensure_ascii=False, indent=4)
            print(f"  Reseñas guardadas en: {path_out}")

    def _extract_reviews_from_site(self, page: Page, url: str, nombre_sitio: str):
        """
        Extrae reseñas de un sitio específico usando Playwright.
        """
        page.goto(url)
        page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_MEDIUM_MIN, Settings.WAIT_MEDIUM_MAX)))

        # --- Detector de bloqueo por Foursquare ---
        if "Sorry! We're having technical difficulties." in page.content():
            print("Bloqueo detectado. Pausando scraping por 10 minutos...")
            page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_EXTRA_LONG_MIN, Settings.WAIT_EXTRA_LONG_MAX)) * 15)  # 10 minutos aprox
            page.reload()
            page.wait_for_timeout(int(np.random.uniform(Settings.WAIT_MEDIUM_MIN, Settings.WAIT_MEDIUM_MAX)))
            # Reintentar una vez
            if "Sorry! We're having technical difficulties." in page.content():
                print("El bloqueo persiste. Saltando este sitio.")
                return []

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
    