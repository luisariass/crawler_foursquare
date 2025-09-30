"""
Scraper de Foursquare que EXTRAE SOLO perfiles de usuario:
  - user_name (texto del <a> del autor)
  - user_url  (href absoluto del perfil)

Incluye:
- Detección de bloqueo por HTML.
- Falla rápida si hay bloqueo, delegando la estrategia al orquestador.
- Esperas por capas sin mezclar engines de selector.
- Click en "Recientes" con varios fallbacks (no bloqueante).
- Reintentos con backoff para errores de red.
- No guarda a disco: delega el guardado al DataHandler.
- Comportamiento humano simulado para evasión.
"""

import time
import random
from typing import Dict, List, Tuple, Any
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from ..config.settings import Settings
from ..utils.human_behavior import HumanBehavior


class FoursquareReviewerScraper:
    """
    Extrae SOLO perfiles de usuario (user_name, user_url) de un sitio Foursquare.
    """

    def __init__(self) -> None:
        self.settings = Settings()
        self.human_behavior: HumanBehavior = None

    def _is_blocked(self, page: Page) -> bool:
        """Verifica si la página muestra el error de bloqueo de Foursquare."""
        try:
            block_selector = self.settings.SELECTORS.get("block_error_h1")
            if not block_selector:
                return False
            block_elem = page.query_selector(block_selector)
            return block_elem is not None
        except Exception:
            return False

    def extract_reviews(
        self,
        page: Page,
        site_url: str,
        site_id: str,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Retorna:
          - estado: 'success' | 'no_results' | 'timeout' | 'blocked' | 'error'
          - data:  [{ "user_name": str, "user_url": str }, ...]
        """
        self.human_behavior = HumanBehavior(page)
        reviews_container_selector = "div.tipsSectionBody" # cambio de tipSection a tipsSectionBody en el selector
        no_reviews_selector = ".noTips"

        for attempt in range(1, self.settings.RETRIES + 1):
            try:
                print(f"[TRY] {attempt}/{self.settings.RETRIES} -> {site_id}")
                self.human_behavior.insert_human_delay(0.5)
                page.goto(site_url, timeout=self.settings.TIMEOUT)

                if self._is_blocked(page):
                    print(f"[BLOCK] Sitio {site_id} bloqueado. Abortando tarea.")
                    return ("blocked", [])

                self.human_behavior.human_like_scroll()

                page.wait_for_selector(
                    f"{reviews_container_selector}, {no_reviews_selector}",
                    timeout=10000
                )

                try:
                    btn_role = page.get_by_role("button", name="Recientes")
                    if btn_role.is_visible():
                        self.human_behavior.human_like_click(
                            'button:has-text("Recientes")'
                        )
                        print("[INFO] Aplicado orden 'Recientes'.")
                except Exception:
                    print("[WARN] No se pudo clickear 'Recientes'.")

                if page.locator(no_reviews_selector).is_visible():
                    data = self._extract_user_profiles_from_page(page)
                    return ("no_results" if not data else "success", data)
    
                if page.locator(reviews_container_selector).count() > 0:
                    data = self._extract_user_profiles_from_page(page)
                    return ("success", data)

                print("[INFO] Estado incierto; reintento suave…")
                self.human_behavior.insert_human_delay(0.5)

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] intento {attempt} sitio {site_id}")
                if attempt == self.settings.RETRIES:
                    return ("timeout", [])
                # Backoff exponencial con jitter
                wait_time = (self.settings.BACKOFF_FACTOR ** attempt) + random.uniform(0, 1)
                print(f"Esperando {wait_time:.2f}s antes de reintentar...")
                time.sleep(wait_time)

            except Exception as e:
                print(f"[ERROR] inesperado en {site_id}: {e}")
                return ("error", [])

        return ("error", [])

    def _extract_user_profiles_from_page(self, page: Page) -> List[Dict[str, str]]:
        """
        Extrae los perfiles de usuario de la página actual.
        """
        results: List[Dict[str, str]] = []
        anchors = page.query_selector_all("span.userName a")
        for a in anchors:
            name = (a.inner_text() or "").strip()
            href = a.get_attribute("href") or ""
            if href.startswith("/"):
                href = f"{self.settings.BASE_URL}{href}"
            if name and href:
                results.append({"user_name": name, "user_url": href})

        seen = set()
        unique_results = []
        for item in results:
            url = item.get("user_url", "")
            if url and url not in seen:
                unique_results.append(item)
                seen.add(url)
        return unique_results