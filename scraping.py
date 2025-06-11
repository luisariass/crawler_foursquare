from playwright.sync_api import sync_playwright
import pandas as pd
import json
from datetime import datetime
import numpy as np
import os
import threading
import queue
import time
import random

# Configuraciones globales
RESULTADOS_DIR = "resultados"
TIPS_DIR = os.path.join(RESULTADOS_DIR, "tips")
USERS_DIR = os.path.join(RESULTADOS_DIR, "users")
PROGRESO_PATH = "progreso_reseñas_usuarios.json"
MAX_WORKERS = 2  # Número de navegadores en paralelo
TAMAÑO_LOTE = 10  # Usuarios por lote

# Crear directorios de resultados
os.makedirs(TIPS_DIR, exist_ok=True)
os.makedirs(USERS_DIR, exist_ok=True)

# Colas para comunicación entre threads
cola_usuarios = queue.Queue()
cola_resultados = queue.Queue()

lock = threading.Lock()  # Para proteger processed_user_ids global

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
    if not os.path.exists(cookies_path):
        print(f"No se encontró el archivo de cookies: {cookies_path}")
        return False
    with open(cookies_path, "r", encoding="utf-8") as f:
        cookies = json.load(f)
    page.context.add_cookies(cookies)
    print("Cookies cargadas correctamente.")
    return True

def extraer_tips_usuario(page, info, processed_user_ids):
    url = info['url_usuario']
    user_id = url.split('/')[-1]
    user_info = {'user': info['nombre_usuario'], 'user_id': user_id}
    
    # Ir a perfil de usuario
    page.goto(url)
    page.wait_for_timeout(np.random.uniform(4000, 6000))
    
    # Encontrar botón "Ver todos los tips"
    see_all = page.query_selector_all('.userTipsHeader > button')
    if not see_all:
        see_all = page.query_selector_all('.userTipsHeader > a')
    see_all = see_all[-1] if see_all else None
    if not see_all:
        print(f"Error: No se encontró el botón 'Ver todos los tips' para {info['nombre_usuario']}")
        return {"user_info": user_info, "tips": []}
    
    # Hacer clic con reintento
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
    
    # Obtener ubicación del usuario
    user_location = page.query_selector('.userLocation')
    user_location = user_location.inner_text() if user_location else None
    
    # Verificar paginación
    total_pages = 1
    if pages := page.query_selector_all('.paginationComponent.page'):
        try:
            last_page = [p for p in pages if p.inner_text().isdigit()][-1]
            total_pages = int(last_page.inner_text())
        except (ValueError, IndexError):
            print("Error getting total pages")
    
    user_tips = []
    # Procesar cada página de tips
    for page_number in range(1, total_pages + 1):
        try:
            page.wait_for_selector('.tipsContainerAll', timeout=10000)
            tips_container = page.query_selector('.tipsContainerAll')
            if not tips_container:
                print(f"Error: No se encontró el contenedor de tips para {info['nombre_usuario']}")
                break
                
            # Extraer todos los tips de esta página
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
            
            # Pasar a la siguiente página si hay más
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

def worker_navegador(worker_id, lote_usuarios, progress_idx_start, processed_user_ids):
    """Función que procesa un lote de usuarios en un navegador dedicado"""
    print(f"Worker {worker_id} iniciando procesamiento de {len(lote_usuarios)} usuarios")
    
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()
        
        # Cargar cookies y navegar a la página inicial
        if not cargar_cookies_playwright(page, "cookies_foursquare.json"):
            print(f"Worker {worker_id}: Error cargando cookies")
            return
        
        # Posicionar ventana y simular comportamiento humano
        page.bring_to_front()
        # Posicionar en cascada - cada navegador en posición diferente
        page.evaluate(f"window.moveTo({200 * worker_id}, {100 * worker_id})")
        # Simular un pequeño movimiento del mouse
        page.mouse.move(200 + 50 * worker_id, 150 + 50 * worker_id)
        
        page.goto('https://es.foursquare.com/')
        page.wait_for_timeout(np.random.uniform(3000, 5000))
        
        # Procesar cada usuario del lote
        for i, info in enumerate(lote_usuarios):
            user_id = info['url_usuario'].split('/')[-1]
            
            # Verificar si el usuario ya fue procesado
            with lock:
                if user_id in processed_user_ids:
                    print(f"Usuario {user_id} ya procesado, saltando...")
                    continue
            
            # Asegurar que la ventana esté activa antes de navegar al perfil
            page.bring_to_front()
            page.mouse.move(250 + 10 * worker_id, 150 + 10 * worker_id)
            
            # Extraer tips del usuario
            resultado = extraer_tips_usuario(page, info, processed_user_ids)
            
            if resultado["tips"]:
                # Guardar los tips del usuario en un archivo dedicado
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                nombre_usuario = info['nombre_usuario'].replace(' ', '_').replace('/', '_')
                tips_path = os.path.join(TIPS_DIR, f'tips_{nombre_usuario}_{user_id}.json')
                users_path = os.path.join(USERS_DIR, f'user_{nombre_usuario}_{user_id}.json')
                
                with open(tips_path, 'w', encoding='utf-8') as file:
                    json.dump(resultado["tips"], file, ensure_ascii=False, indent=4)
                with open(users_path, 'w', encoding='utf-8') as file:
                    json.dump(resultado["user_info"], file, ensure_ascii=False, indent=4)
                
            # Actualizar progreso global
            with lock:
                processed_user_ids.add(user_id)
                progress_idx = progress_idx_start + i + 1
                guardar_progreso(progress_idx, processed_user_ids)
            
            # Esperar entre usuarios (tiempo aleatorio para simular comportamiento humano)
            page.wait_for_timeout(int(np.random.uniform(20000, 30000)))
        
        browser.close()
    
    print(f"Worker {worker_id} completado")

def main():
    # Cargar lista de usuarios desde CSV sin duplicados
    try:
        urls_reseñantes = pd.read_csv('merge_user_altlantico_bolivar_no_duplicates.csv', sep=',')
    except Exception as e:
        print(f"Error cargando CSV: {e}")
        return
    
    # Cargar progreso
    progreso = cargar_progreso()
    start_idx = progreso["idx_actual"] if progreso and "idx_actual" in progreso else 0
    processed_user_ids = set(progreso["processed_user_ids"]) if progreso and "processed_user_ids" in progreso else set()
    
    print(f"Iniciando desde el índice {start_idx}, {len(processed_user_ids)} usuarios ya procesados")
    
    # Filtrar usuarios ya procesados del CSV
    usuarios_pendientes = []
    for idx, (_, info) in enumerate(urls_reseñantes.iterrows()):
        if idx < start_idx:
            continue
        
        user_id = info['url_usuario'].split('/')[-1]
        if user_id not in processed_user_ids:
            usuarios_pendientes.append(info)
    
    print(f"Total de usuarios pendientes: {len(usuarios_pendientes)}")
    
    # Dividir en lotes para los workers
    lotes = []
    idx = 0
    while idx < len(usuarios_pendientes):
        lote = usuarios_pendientes[idx:idx+TAMAÑO_LOTE]
        lotes.append(lote)
        idx += TAMAÑO_LOTE
    
    print(f"Dividido en {len(lotes)} lotes")
    
    # Procesar lotes en grupos de MAX_WORKERS
    for grupo_idx in range(0, len(lotes), MAX_WORKERS):
        threads = []
        # Tomar solo MAX_WORKERS lotes para este grupo
        grupo_lotes = lotes[grupo_idx:grupo_idx + MAX_WORKERS]
        
        print(f"Procesando grupo {grupo_idx//MAX_WORKERS + 1} con {len(grupo_lotes)} navegadores")
        
        # Iniciar los navegadores para este grupo
        for i, lote in enumerate(grupo_lotes):
            progress_idx_start = start_idx + ((grupo_idx + i) * TAMAÑO_LOTE)
            worker = threading.Thread(
                target=worker_navegador, 
                args=(grupo_idx + i, lote, progress_idx_start, processed_user_ids)
            )
            threads.append(worker)
            worker.start()
            # Diferir un poco el inicio de cada navegador
            time.sleep(5)
        
        # Esperar a que TODOS los navegadores de este grupo terminen
        # antes de iniciar el siguiente grupo
        for thread in threads:
            thread.join()
            
        print(f"Grupo {grupo_idx//MAX_WORKERS + 1} completado")
    
    print("Todos los usuarios han sido procesados")

if __name__ == "__main__":
    main()