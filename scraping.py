from playwright.sync_api import sync_playwright
import pandas as pd
import json
from datetime import datetime
import numpy as np
import os

urls_reseñantes = pd.read_csv('merge_user_altlantico_bolivar_no_duplicates.csv', sep=',')

credentials = {}
all_tips = []
proccessed_users = []

PROGRESO_PATH = "progreso_reseñas_usuarios.json"

def guardar_progreso(idx_actual, processed_user_ids):
    with open(PROGRESO_PATH, "w", encoding="utf-8") as f:
        json.dump({"idx_actual": idx_actual, "processed_user_ids": list(processed_user_ids)}, f, ensure_ascii=False, indent=4)
    print(f"Progreso guardado en {PROGRESO_PATH}")

def cargar_progreso():
    if os.path.exists(PROGRESO_PATH):
        with open(PROGRESO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def cargar_cookies_playwright(page, cookies_path="cookies_foursquare.json"):
    import json
    import os
    if not os.path.exists(cookies_path):
        print(f"No se encontró el archivo de cookies: {cookies_path}")
        return False
    with open(cookies_path, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    page.context.add_cookies(cookies)
    print("Cookies cargadas correctamente.")
    return True

"""
credentials.txt format
username=your_username
password=your_password
"""

def save_data():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(f'tips_data_{timestamp}.json', 'w', encoding='utf-8') as file:
        json.dump(all_tips, file, ensure_ascii=False, indent=4)
    with open(f'processed_users_{timestamp}.json', 'w', encoding='utf-8') as file:
        json.dump(proccessed_users, file, ensure_ascii=False, indent=4)
    print(f"Datos guardados en tips_data_{timestamp}.json")

browser = None
page = None

try:
    progreso = cargar_progreso()
    start_idx = progreso["idx_actual"] if progreso and "idx_actual" in progreso else 0
    processed_user_ids = set(progreso["processed_user_ids"]) if progreso and "processed_user_ids" in progreso else set()

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()
        # Cargar cookies antes de navegar
        if not cargar_cookies_playwright(page, "cookies_foursquare.json"):
            print("No se pudieron cargar las cookies. Ejecuta primero el script para guardarlas.")
            exit(1)
        page.goto('https://es.foursquare.com/')
        page.wait_for_timeout(np.random.uniform(3000, 5000))

        for idx, (_, info) in enumerate(urls_reseñantes.iterrows()):
            if idx < start_idx:
                continue

            url = info['url_usuario']
            user_id = url.split('/')[-1]
            user_info = {'user': info['nombre_usuario'], 'user_id': user_id}

            # Control de duplicados
            if user_id in processed_user_ids:
                print(f"Usuario {user_id} ya procesado, saltando...")
                continue

            page.goto(url)
            page.wait_for_timeout(np.random.uniform(5000, 6000))
            #Find all tips button or recent tips button
            see_all = page.query_selector_all('.userTipsHeader > button')
            if not see_all:
                see_all = page.query_selector_all('.userTipsHeader > a')
            see_all = see_all[-1] if see_all else None
            if not see_all:
                print(f"Error: No se encontró el botón 'Ver todos los tips' para {info['nombre_usuario']}")
                proccessed_users.append(user_info)
                processed_user_ids.add(user_id)
                guardar_progreso(idx + 1, processed_user_ids)
                continue
            see_all.click()
            page.wait_for_timeout(np.random.uniform(5000, 6000))

            user_location = page.query_selector('.userLocation')
            user_location = user_location.inner_text() if user_location else None

            # Check if the tips sections is paginated
            total_pages = 1
            if pages := page.query_selector_all('.paginationComponent.page'):
                try:
                    last_page = [p for p in pages if p.inner_text().isdigit()][-1]
                    total_pages = int(last_page.inner_text())
                except (ValueError, IndexError):
                    print("Error getting total pages")
            user_tips = []
            # If the tips are paginated, loop through each page
            # If not, process the tips directly
            for page_number in range(1, total_pages + 1):
                page.wait_for_selector(f'.tipsContainerAll', timeout=10000)
                # Select the tips container
                tips_container = page.query_selector('.tipsContainerAll')
                if not tips_container:
                    print(f"Error: No se encontró el contenedor de tips para {info['nombre_usuario']}")
                    proccessed_users.append(user_info)
                    processed_user_ids.add(user_id)
                    guardar_progreso(idx + 1, processed_user_ids)
                    break

                # Select all tips within the container
                tips = tips_container.query_selector_all('.tipCard')
                try:
                    # Loop through each tip and extract the required information
                    for tip in tips:
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
                    print(f"Error al procesar los tips: {e}")
                # If the tips are paginated, click on the next page button
                if total_pages > 1 and page_number < total_pages:
                    next_page = page.query_selector(f'.paginationComponent.page.page{page_number + 1}')
                    if next_page:
                        next_page.click()
                        page.wait_for_timeout(np.random.uniform(3000, 5000))
                    else:
                        print(f"Error: No se encontró el botón de siguiente página {page_number + 1} para {info['nombre_usuario']}")
                        break

            # Save the user tips to the all_tips list
            all_tips.extend(user_tips)
            proccessed_users.append(user_info)
            processed_user_ids.add(user_id)
            print(f"Reseñante: {info['nombre_usuario']}, ID: {user_id}, Total de tips: {len(user_tips)}")

            # Guardar progreso después de cada usuario
            guardar_progreso(idx + 1, processed_user_ids)

            # Save data every 20 users processed
            if (idx + 1) % 20 == 0:
                print(f"Guardando datos cada 20 usuarios procesados")
                save_data()
                page.wait_for_timeout(int(np.random.uniform(30_000, 40_000)))
            page.wait_for_timeout(int(np.random.uniform(30_000, 40_000)))
except Exception as e:
    print(f"Error general: {e}")
finally:
    print("Guardando datos")
    # Save any remaining data before exiting
    save_data()
    print("Fin del programa")