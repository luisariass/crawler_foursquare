"""
Scraper de Foursquare que EXTRAe SOLO perfiles de usuario:
  - user_name (texto del <a> del autor)
  - user_url  (href absoluto del perfil)

Incluye:
- Detección de bloqueo por HTML ("Sorry! We're having technical difficulties.")
- Pausa aleatoria + reload si hay bloqueo
- Esperas por capas sin mezclar engines de selector
- Click en "Recientes" con varios fallbacks (no bloqueante)
- Reintentos con backoff
- No guarda a disco: delega el guardado al DataHandler
"""

import time
import numpy as np
from typing import Dict, List, Tuple, Any
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from ..config.settings import Settings
from ..utils.helpers import current_timestamp  # si lo usas en logs/razones


class FoursquareReviewerScraper:
    """
    Extrae SOLO perfiles de usuario (user_name, user_url) de un sitio Foursquare.
    """

    def __init__(self) -> None:
        self.settings = Settings()

    def extract_reviews(
        self,
        page: Page,
        site_url: str,
        site_id: str,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Retorna:
          - estado: 'success' | 'no_results' | 'timeout' | 'generic_error' | 'error'
          - data:  [{ "user_name": str, "user_url": str }, ...]
        """
        BLOCK_TEXT = "Sorry! We're having technical difficulties."
        reviews_container_selector = "div.tipSection"
        no_reviews_selector = ".noTips"

        for attempt in range(1, self.settings.RETRIES + 1):
            try:
                print(f"[TRY] {attempt}/{self.settings.RETRIES} -> {site_id}")
                page.goto(site_url, timeout=self.settings.TIMEOUT)
                page.wait_for_timeout(int(np.random.uniform(3000, 5000)))

                # 1) Detección de bloqueo por HTML (robusto)
                html = page.content()
                if BLOCK_TEXT in html:
                    print("[BLOCK] Banner detectado. Pausa y reload…")
                    page.wait_for_timeout(
                        int(np.random.uniform(
                            self.settings.WAIT_EXTRA_LONG_MIN,
                            self.settings.WAIT_EXTRA_LONG_MAX
                        )) * 5  # pausa larga (similar a tu extractor)
                    )
                    page.reload()
                    page.wait_for_timeout(int(np.random.uniform(2000, 4000)))
                    if BLOCK_TEXT in page.content():
                        # No tumbar el job superior: solo marca el sitio con generic_error
                        return ("generic_error", [])

                # 2) Espera por “aparición” de algún estado conocido (sin mezclar engines)
                appeared = False
                for _ in range(20):  # ~2s (20 * 100ms)
                    if page.locator(reviews_container_selector).count() > 0:
                        appeared = True; break
                    if page.locator(no_reviews_selector).count() > 0:
                        appeared = True; break
                    if page.get_by_text(BLOCK_TEXT, exact=True).count() > 0:
                        appeared = True; break
                    page.wait_for_timeout(100)

                # 3) Intento de ordenar por "Recientes" (no bloqueante si falla)
                try:
                    clicked = False
                    # a) Accesible/rol
                    btn_role = page.get_by_role("button", name="Recientes")
                    if btn_role and btn_role.is_visible():
                        btn_role.click(timeout=1500); clicked = True
                    # b) CSS con :has-text()
                    if not clicked:
                        css = "span.sortLink:has-text('Recientes')"
                        loc = page.locator(css).first
                        if loc.is_visible():
                            loc.click(timeout=1500); clicked = True
                    # c) XPath (como en tu extractor)
                    if not clicked:
                        xp = '//span[contains(@class,"sortLink") and contains(text(),"Recientes")]'
                        loc = page.locator(xp).first
                        if loc.is_visible():
                            loc.click(timeout=1500); clicked = True
                    if clicked:
                        print("[INFO] Aplicado orden 'Recientes'.")
                        page.wait_for_timeout(int(np.random.uniform(1500, 3000)))
                except Exception as e:
                    print(f"[WARN] No se pudo clickear 'Recientes': {e}")

                # 4) Estados finales
                if page.get_by_text(BLOCK_TEXT, exact=True).is_visible():
                    return ("generic_error", [])

                if page.locator(no_reviews_selector).is_visible():
                    # Aun así, intentamos extraer por si hay perfiles en otra zona
                    data = self._extract_user_profiles_from_page(page)
                    # Si no hay nada, es no_results; si hay algo, mejor reportarlo
                    return ("no_results" if not data else "success", data)

                # 5) Extracción mínima de usuarios
                has_container = page.locator(reviews_container_selector).count() > 0
                has_tips = len(page.query_selector_all("div.tipContents")) > 0
                if has_container or has_tips:
                    data = self._extract_user_profiles_from_page(page)
                    return ("success", data)

                # Si nada claro, breve espera y reintento
                print("[INFO] Estado incierto; reintento suave…")
                page.wait_for_timeout(int(np.random.uniform(500, 1200)))

            except PlaywrightTimeoutError:
                print(f"[TIMEOUT] intento {attempt} sitio {site_id}")
                if attempt == self.settings.RETRIES:
                    return ("timeout", [])
                time.sleep(self.settings.BACKOFF_FACTOR * attempt)

            except Exception as e:
                print(f"[ERROR] inesperado en {site_id}: {e}")
                return ("error", [])

        return ("error", [])

    # -----------------------
    # Helpers
    # -----------------------

    def _extract_user_profiles_from_page(self, page: Page) -> List[Dict[str, str]]:
        """
        Devuelve SOLO:
          - user_name (texto del enlace del usuario)
          - user_url  (href absoluto al perfil)

        Estas claves son las que espera tu UsersStorageStrategy/DataHandler:
          - unique id: user_url
          - data key en archivo: perfiles_usuarios
        """
        results: List[Dict[str, str]] = []

        anchors = page.query_selector_all("span.userName a")
        for a in anchors:
            name = (a.inner_text() or "").strip()
            href = a.get_attribute("href") or ""
            if href.startswith("/"):
                href = f"{self.settings.BASE_URL}{href}"
            if name or href:
                results.append({"user_name": name, "user_url": href})

        # (Opcional) dedupe rápido por user_url dentro de la misma página:
        seen = set()
        uniq = []
        for it in results:
            url = it.get("user_url", "")
            if url and url not in seen:
                uniq.append(it)
                seen.add(url)
        return uniq
