from playwright.sync_api import sync_playwright
import pandas as pd
import json
from datetime import datetime
import numpy as np
import os
import time

# Cargar URLs o sitios a procesar
urls_sitios = pd.read_csv('merge_sities.csv', sep=',')

credentials = {}
all_sitios = {}  # Cambiar a diccionario para organizar por municipio
processed_urls = {}  # Cambiar a diccionario para organizar por municipio

def obtener_sitios_turisticos_playwright(page, url, carpeta_salida="sitios_turisticos"):
    """
    Extrae sitios turísticos usando Playwright en lugar de requests y BeautifulSoup
    """
    # Crear la carpeta de salida si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    try:
        # Navegar a la URL
        page.goto(url)
        page.wait_for_timeout(int(np.random.uniform(3000, 5000)))
        
        # Lista para almacenar los sitios turísticos
        sitios_list = []
        
        # Esperar a que carguen los elementos
        page.wait_for_selector('.contentHolder', timeout=10000)
        
        # Buscar todos los elementos que contienen información sobre sitios turísticos
        sitios = page.query_selector_all('.contentHolder')
        
        # Extraer información de cada sitio
        for i, sitio in enumerate(sitios):
            try:
                sitio_data = {
                    "id": i + 1,
                    "puntuacion": "N/A",
                    "nombre": "N/A",
                    "categoria": "N/A",
                    "direccion": "N/A",
                    "url_sitio": "",
                    "usuario_reseña": "N/A",
                    "fecha_reseña": "N/A",
                    "contenido_reseña": "N/A",
                    "fecha_extraccion": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Extraer puntuación
                puntuacion_element = sitio.query_selector('.venueScore.positive')
                if puntuacion_element:
                    sitio_data["puntuacion"] = puntuacion_element.inner_text().strip()
                
                # Extraer nombre
                nombre_element = sitio.query_selector('h2')
                if nombre_element:
                    nombre_link = nombre_element.query_selector('a')
                    if nombre_link:
                        sitio_data["nombre"] = nombre_link.inner_text().strip()
                        # Extraer URL del sitio
                        href = nombre_link.get_attribute('href')
                        if href:
                            if href.startswith('/'):
                                sitio_data["url_sitio"] = f"https://es.foursquare.com{href}"
                            else:
                                sitio_data["url_sitio"] = href
                    else:
                        sitio_data["nombre"] = nombre_element.inner_text().strip()
                
                # Extraer categoría
                categoria_element = sitio.query_selector('.venueDataItem')
                if categoria_element:
                    categoria_text = categoria_element.inner_text().strip()
                    sitio_data["categoria"] = categoria_text.replace('•', '').strip()
                
                # Extraer dirección
                direccion_element = sitio.query_selector('.venueAddress')
                if direccion_element:
                    sitio_data["direccion"] = direccion_element.inner_text().strip()
                
                # Extraer información de reseña
                reseña_element = sitio.query_selector('.tipText')
                if reseña_element:
                    # Extraer autor de la reseña
                    author_element = reseña_element.query_selector('.tipAuthor')
                    if author_element:
                        usuario_element = author_element.query_selector('.userName')
                        if usuario_element:
                            sitio_data["usuario_reseña"] = usuario_element.inner_text().strip()
                        
                        # Extraer fecha (texto completo del autor menos el nombre de usuario)
                        full_author_text = author_element.inner_text().strip()
                        user_text = sitio_data["usuario_reseña"]
                        potential_date_text = full_author_text.replace(user_text, '', 1).strip()
                        if potential_date_text.startswith('•'):
                            sitio_data["fecha_reseña"] = potential_date_text[1:].strip()
                        else:
                            sitio_data["fecha_reseña"] = potential_date_text
                    
                    # Extraer contenido de la reseña
                    full_tip_text = reseña_element.inner_text().strip()
                    if author_element:
                        author_text = author_element.inner_text().strip()
                        sitio_data["contenido_reseña"] = full_tip_text.replace(author_text, '', 1).strip()
                    else:
                        sitio_data["contenido_reseña"] = full_tip_text
                
                sitios_list.append(sitio_data)
                
            except Exception as e:
                print(f"Error al procesar sitio {i + 1}: {e}")
                continue
        
        return sitios_list
        
    except Exception as e:
        print(f"Error al acceder a la página {url}: {e}")
        return []

def save_municipio_data(municipio, carpeta_salida="sitios_turisticos"):
    """Guarda los datos de un municipio específico"""
    if municipio not in all_sitios or not all_sitios[municipio]:
        return
    
    # Crear la carpeta de salida si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # Crear diccionario con todos los datos del municipio
    datos = {
        "municipio": municipio,
        "sitios_turisticos": all_sitios[municipio],
        "total": len(all_sitios[municipio]),
        "fecha_extraccion": time.strftime("%Y-%m-%d %H:%M:%S"),
        "url_procesada": processed_urls.get(municipio, {})
    }
    
    # Nombre del archivo por municipio
    archivo_municipio = os.path.join(carpeta_salida, f'sitios_{municipio}.json')
    
    # Guardar en JSON
    with open(archivo_municipio, 'w', encoding='utf-8') as file:
        json.dump(datos, file, ensure_ascii=False, indent=4)
    
    print(f"Datos del municipio {municipio} guardados en {archivo_municipio}")

def save_all_data(carpeta_salida="sitios_turisticos"):
    """Guarda los datos de todos los municipios procesados"""
    # Crear la carpeta de salida si no existe
    os.makedirs(carpeta_salida, exist_ok=True)
    
    # Guardar archivo individual por cada municipio
    for municipio in all_sitios.keys():
        save_municipio_data(municipio, carpeta_salida)
    
    # Crear resumen general
    total_sitios = sum(len(sitios) for sitios in all_sitios.values())
    resumen = {
        "resumen_general": {
            "total_municipios_procesados": len(all_sitios),
            "total_sitios_extraidos": total_sitios,
            "fecha_procesamiento": time.strftime("%Y-%m-%d %H:%M:%S")
        },
        "municipios": {municipio: len(sitios) for municipio, sitios in all_sitios.items()},
        "urls_procesadas": processed_urls
    }
    
    archivo_resumen = os.path.join(carpeta_salida, 'resumen_extraccion.json')
    with open(archivo_resumen, 'w', encoding='utf-8') as file:
        json.dump(resumen, file, ensure_ascii=False, indent=4)
    
    print(f"Resumen general guardado en {archivo_resumen}")

# Cargar credenciales
with open('credentials.txt', 'r') as file:
    for line in file:
        key, value = line.strip().split('=')
        credentials[key] = value

browser = None
page = None

start = 0
end = min(len(urls_sitios) - 1, 4)  # Procesar las primeras 5 URLs

try:
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()
        
        # Login en Foursquare
        page.goto('https://es.foursquare.com/login')
        page.wait_for_timeout(int(np.random.uniform(5000, 6000)))
        page.fill('input[id="username"]', credentials['username'])
        page.fill('input[id="password"]', credentials['password'])
        page.click('input[id="loginFormButton"]')
        page.pause()  # Pause for 2FA code
        
        # Procesar cada URL del CSV
        for idx, (_, info) in enumerate(urls_sitios.iloc[start:end+1].iterrows()):
            url = info['url_municipio']
            municipio = info['municipio']
            
            print(f"Procesando municipio {idx + 1}: {municipio} - {url}")
            
            # Inicializar estructuras para el municipio si no existen
            if municipio not in all_sitios:
                all_sitios[municipio] = []
            
            # Extraer sitios turísticos de la página
            sitios_encontrados = obtener_sitios_turisticos_playwright(page, url)
            
            if sitios_encontrados:
                # Verificar duplicados por URL del sitio
                sitios_existentes_urls = {sitio.get('url_sitio', '') for sitio in all_sitios[municipio]}
                sitios_nuevos = []
                
                for sitio in sitios_encontrados:
                    if sitio.get('url_sitio', '') not in sitios_existentes_urls:
                        sitio['fuente_url'] = url
                        sitio['municipio'] = municipio
                        sitio['indice_procesamiento'] = idx + 1
                        sitios_nuevos.append(sitio)
                
                all_sitios[municipio].extend(sitios_nuevos)
                
                # Re-enumerar los IDs para mantener secuencia
                for i, sitio in enumerate(all_sitios[municipio]):
                    sitio['id'] = i + 1
                
                processed_urls[municipio] = {
                    'url': url,
                    'sitios_encontrados': len(sitios_nuevos),
                    'sitios_duplicados_omitidos': len(sitios_encontrados) - len(sitios_nuevos),
                    'total_sitios_municipio': len(all_sitios[municipio]),
                    'fecha_procesamiento': time.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                print(f"Se encontraron {len(sitios_nuevos)} sitios nuevos en {municipio}")
                print(f"Total de sitios únicos en {municipio}: {len(all_sitios[municipio])}")
                
                # Guardar datos del municipio inmediatamente
                save_municipio_data(municipio)
                
            else:
                print(f"No se encontraron sitios en {municipio}")
                processed_urls[municipio] = {
                    'url': url,
                    'sitios_encontrados': 0,
                    'fecha_procesamiento': time.strftime("%Y-%m-%d %H:%M:%S"),
                    'error': 'No se encontraron sitios'
                }
            
            # Guardar resumen cada 5 URLs procesadas
            if (idx + 1) % 5 == 0:
                print(f"Guardando resumen cada 5 URLs procesadas")
                save_all_data()
                page.wait_for_timeout(int(np.random.uniform(30_000, 40_000)))
            
            # Espera entre URLs
            page.wait_for_timeout(int(np.random.uniform(15_000, 25_000)))

except Exception as e:
    print(f"Error general: {e}")
finally:
    print("Guardando datos finales")
    save_all_data()
    
    total_sitios = sum(len(sitios) for sitios in all_sitios.values())
    print(f"Fin del programa. Total de sitios extraídos: {total_sitios}")
    print(f"Municipios procesados: {list(all_sitios.keys())}")