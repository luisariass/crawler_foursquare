"""
Clase para extraer las URLs de los perfiles de los reseñantes de un sitio.
Sigue el mismo patrón de diseño resiliente y sin estado que scraper.py.
"""
import time
import numpy as np
from typing import Dict, List, Tuple
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from ..config.settings import Settings
from ..utils.helpers import current_timestamp


class FoursquareReviewerScraper:
    """
    Extrae las URLs de los perfiles de los reseñantes de un sitio en Foursquare.
    """

    def __init__(self) -> None:
        """Inicializa el scraper de URLs de reseñantes."""
        self.settings = Settings()

    def extract_reviewer_urls(
        self,
        page: Page,
        site_url: str,
        site_id: str
    ) -> Tuple[str, List[Dict[str, str]]]:
        """
        Extrae las URLs de los perfiles de los reseñantes de un sitio.

        Aplica un bucle de reintentos y detección de estados para robustez.

        Args:
            page: La instancia de la página de Playwright.
            site_url: La URL del sitio a procesar.
            site_id: El ID del sitio para logging y contexto.

        Returns:
            Una tupla con el estado ('success', 'no_results', 'error', etc.)
            y una lista de diccionarios, cada uno con la URL del perfil.
        """
        for attempt in range(1, self.settings.RETRIES + 1):
            try:
                print(
                    f"Intento {attempt}/{self.settings.RETRIES} para sitio ID: "
                    f"{site_id}"
                )
                page.goto(site_url, timeout=self.settings.TIMEOUT)
                page.wait_for_timeout(int(np.random.uniform(3000, 5000)))

                # --- Detección de Múltiples Estados ---
                reviews_container_selector = '.tipSection'
                no_reviews_selector = '.noTips'
                error_selector = self.settings.SELECTORS['generic_error_card']

                page.locator(
                    f"{reviews_container_selector}, {no_reviews_selector}, "
                    f"{error_selector}"
                ).first.wait_for(timeout=20000)

                # 1. Estado: Error genérico (bloqueo)
                if page.is_visible(error_selector):
                    print(f"[BLOCK] Bloqueo del servidor en sitio {site_id}")
                    self._register_failed_site(site_id, site_url, "generic_error")
                    return ("generic_error", [])

                # 2. Estado: Sin resultados (mensaje explícito)
                elif page.is_visible(no_reviews_selector):
                    print(f"[INFO] No hay tips/reseñas para el sitio {site_id}.")
                    return ("no_results", [])

                # 3. Estado: Éxito, hay contenedor de reseñas
                elif page.is_visible(reviews_container_selector):
                    reviewer_urls = self._extract_urls_from_page(page)
                    if not reviewer_urls:
                        print(
                            f"[INFO] Contenedor de tips encontrado, pero no "
                            f"se extrajeron URLs de reseñantes para {site_id}."
                        )
                        return ("no_results", [])

                    return ("success", reviewer_urls)

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] Timeout en intento {attempt} para {site_id}.")
                if attempt == self.settings.RETRIES:
                    self._register_failed_site(site_id, site_url, "timeout_final")
                    return ("timeout", [])
                time.sleep(self.settings.BACKOFF_FACTOR * attempt)

            except Exception as e:
                print(f"[ERROR] Error inesperado para sitio {site_id}: {e}")
                self._register_failed_site(site_id, site_url, f"error: {e}")
                return ("error", [])

        return ("error", [])

    def _extract_urls_from_page(self, page: Page) -> List[Dict[str, str]]:
        """
        Extrae las URLs de los perfiles de la página actual.
        Intenta hacer clic en 'Recientes' si está disponible.
        """
        # --- Lógica Generalizada para Casos 2 y 3 ---
        # Intenta hacer clic en el botón "Recientes" si existe, pero no falla si no está.
        try:
            recientes_btn_selector = '//span[contains(@class, "sortLink") and contains(text(), "Recientes")]'
            if page.query_selector(recientes_btn_selector):
                page.click(recientes_btn_selector)
                print("[INFO] Botón 'Recientes' encontrado y clickeado.")
                page.wait_for_timeout(int(np.random.uniform(2000, 4000)))
        except Exception as e:
            # Este error no es crítico, solo informativo.
            print(f"[WARN] No se pudo hacer clic en 'Recientes': {e}")

        # Ahora, extrae todos los enlaces de usuario visibles en la página.
        reviewer_link_selector = 'span.userName a'
        reviewer_links = page.query_selector_all(reviewer_link_selector)

        urls = []
        base_url = self.settings.BASE_URL

        for link in reviewer_links:
            href = link.get_attribute('href')
            if href:
                full_url = f"{base_url}{href}" if href.startswith('/') else href
                urls.append({'user_url': full_url})

        # Eliminar duplicados basados en 'user_url'
        unique_urls = list({item['user_url']: item for item in urls}.values())
        return unique_urls

    def _register_failed_site(
        self,
        site_id: str,
        site_url: str,
        reason: str
    ) -> None:
        """Registra un sitio que falló en la extracción de URLs de reseñantes."""
        failed_path = self.settings.FAILED_REVIEW_SITES_PATH
        with open(failed_path, "a", encoding="utf-8") as f:
            f.write(f"{site_id},{site_url},{reason},{current_timestamp()}\n")
        print(f"[FAILED] Sitio de reseña registrado: {site_id} - Razón: {reason}")