import os
import json
import pandas as pd
import numpy as np
from playwright.sync_api import sync_playwright
from config.settings import Settings
from utils.helpers import guardar_progreso, cargar_progreso, save_log_error
from utils.cookies_helper import cargar_cookies_playwright

def process_user(info):
    """
    Función para ser ejecutada en paralelo por cada usuario.
    """
    with sync_playwright() as p:
        browser = getattr(p, Settings.BROWSER_TYPE).launch(headless=Settings.HEADLESS)
        page = browser.new_page()
        if not cargar_cookies_playwright(page, Settings.COOKIES_PATH):
            print("Error cargando cookies")
            browser.close()
            return info['url_usuario'].split('/')[-1], False
        extractor = UserReviewsExtractor()
        resultado = extractor._extract_reviews_from_user(page, info)
        user_id = info['url_usuario'].split('/')[-1]
        if resultado["tips"]:
            tips_path = os.path.join(Settings.TIPS_DIR, f'tips_{user_id}.json')
            users_path = os.path.join(Settings.USERS_DIR, f'user_{user_id}.json')
            with open(tips_path, 'w', encoding='utf-8') as file:
                json.dump(resultado["tips"], file, ensure_ascii=False, indent=4)
            with open(users_path, 'w', encoding='utf-8') as file:
                json.dump(resultado["user_info"], file, ensure_ascii=False, indent=4)
        browser.close()
        return user_id, True

class UserReviewsExtractor:
    def __init__(self):
        Settings.create_output_dirs()

    def extract_reviews_from_csv(self, csv_path: str, max_workers: int = 2) -> None:
        try:
            df = pd.read_csv(csv_path, sep=',')
        except Exception as e:
            print(f"Error cargando CSV {csv_path}: {e}")
            return

        progreso = cargar_progreso()
        start_idx = progreso["idx_actual"] if progreso and "idx_actual" in progreso else 0
        processed_user_ids = set(progreso["processed_user_ids"]) if progreso and "processed_user_ids" in progreso else set()

        print(f"Iniciando desde el índice {start_idx}, {len(processed_user_ids)} usuarios ya procesados")

        usuarios_pendientes = []
        for idx, (_, info) in enumerate(df.iterrows()):
            if idx < start_idx:
                continue
            user_id = info['url_usuario'].split('/')[-1]
            if user_id not in processed_user_ids:
                usuarios_pendientes.append(info)

        print(f"Total de usuarios pendientes: {len(usuarios_pendientes)}")

        from concurrent.futures import ProcessPoolExecutor, as_completed

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_user, info): info for info in usuarios_pendientes}
            for idx, future in enumerate(as_completed(futures)):
                user_id, success = future.result()
                processed_user_ids.add(user_id)
                guardar_progreso(start_idx + idx + 1, processed_user_ids)
                print(f"Usuario {user_id} procesado. Éxito: {success}")

        print("Todos los usuarios han sido procesados")

    def _extract_reviews_from_user(self, page, info: dict) -> dict:
        url = info['url_usuario']
        user_id = url.split('/')[-1]
        user_info = {'user': info['nombre_usuario'], 'user_id': user_id}

        max_reintentos = 3
        for intento in range(max_reintentos):
            try:
                page.goto(url, timeout=60000)
                break
            except Exception as e:
                print(f"[{user_id}] Intento {intento+1} falló al navegar a {url}: {e}")
                if intento == max_reintentos - 1:
                    print(f"[{user_id}] No se pudo cargar la página tras {max_reintentos} intentos. Saltando usuario.")
                    return {"user_info": user_info, "tips": []}
                page.wait_for_timeout(5000)

        see_all = page.query_selector_all('.userTipsHeader > button')
        if not see_all:
            see_all = page.query_selector_all('.userTipsHeader > a')
        see_all = see_all[-1] if see_all else None
        if not see_all:
            print(f"Error: No se encontró el botón 'Ver todos los tips' para {info['nombre_usuario']}")
            save_log_error(info)
            return {"user_info": user_info, "tips": []}
        max_intentos = 3
        for intento in range(max_intentos):
            try:
                see_all.click(timeout=10000)
                break
            except Exception as e:
                if intento < max_intentos - 1:
                    print(f"Reintentando clic en 'Ver todos los tips' para {info['nombre_usuario']}: {e}")
                    page.wait_for_timeout(np.random.uniform(2000, 3000))
                else:
                    print(f"No se pudo hacer clic en 'Ver todos los tips' para {info['nombre_usuario']}")
                    return {"user_info": user_info, "tips": []}
        page.wait_for_timeout(np.random.uniform(4000, 6000))
        user_location = page.query_selector('.userLocation')
        user_location = user_location.inner_text() if user_location else None
        total_pages = 1
        if pages := page.query_selector_all('.paginationComponent.page'):
            try:
                last_page = [p for p in pages if p.inner_text().isdigit()][-1]
                total_pages = int(last_page.inner_text())
            except (ValueError, IndexError):
                print("Error getting total pages")
        user_tips = []
        for page_number in range(1, total_pages + 1):
            try:
                page.wait_for_selector('.tipsContainerAll', timeout=10000)
                tips_container = page.query_selector('.tipsContainerAll')
                if not tips_container:
                    print(f"Error: No se encontró el contenedor de tips para {info['nombre_usuario']}")
                    break
                tips = tips_container.query_selector_all('.tipCard')
                for tip in tips:
                    try:
                        tip_info = {}
                        tip_info['user'] = info['nombre_usuario']
                        tip_info['user_id'] = user_id
                        tip_info['user_location'] = user_location
                        date_element = tip.query_selector('.tipDate')
                        tip_info['date'] = date_element.inner_text() if date_element else None
                        place_element = tip.query_selector('.tipVenueInfo > a')
                        tip_info['reviewed_place'] = place_element.inner_text() if place_element else None
                        category_element = tip.query_selector('.category')
                        tip_info['reviewed_category'] = category_element.inner_text() if category_element else None
                        location = None
                        if category_element:
                            sibling_text = category_element.evaluate_handle('el => el.nextSibling')
                            if sibling_text:
                                raw_text = sibling_text.evaluate('n => n.textContent')
                                location = raw_text.strip("· ").strip() if (raw_text and raw_text != '') else None
                        tip_info['reviewed_location'] = location
                        comment_element = tip.query_selector('.tipContent')
                        tip_info['comment'] = comment_element.inner_text() if comment_element else None
                        score_element = tip.query_selector('.venueScore')
                        tip_info['score'] = score_element.inner_text() if score_element else None
                        user_tips.append(tip_info)
                    except Exception as e:
                        print(f"Error procesando un tip: {e}")
                if total_pages > 1 and page_number < total_pages:
                    try:
                        next_page = page.query_selector(f'.paginationComponent.page.page{page_number + 1}')
                        if next_page:
                            next_page.click(timeout=10000)
                            page.wait_for_timeout(np.random.uniform(3000, 5000))
                        else:
                            print(f"Error: No se encontró el botón de siguiente página {page_number + 1}")
                            break
                    except Exception as e:
                        print(f"Error en paginación: {e}")
                        break
            except Exception as e:
                print(f"Error procesando página {page_number}: {e}")
        print(f"Reseñante: {info['nombre_usuario']}, ID: {user_id}, Total de tips: {len(user_tips)}")
        return {"user_info": user_info, "tips": user_tips}