from playwright.sync_api import sync_playwright
import pandas as pd
import json
from datetime import datetime
import numpy as np

urls_reseñantes = pd.read_csv('merge_user.csv', sep=',')

credentials = {}
all_tips = []
proccessed_users = []

"""
credentials.txt format
username=your_username
password=your_password
"""

with open('credentials.txt', 'r') as file:
    for line in file:
        key, value = line.strip().split('=')
        credentials[key] = value

def save_data():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    with open(f'tips_data_{timestamp}.json', 'w', encoding='utf-8') as file:
        json.dump(all_tips, file, ensure_ascii=False, indent=4)
    with open(f'processed_users_{timestamp}.json', 'w', encoding='utf-8') as file:
        json.dump(proccessed_users, file, ensure_ascii=False, indent=4)
    print(f"Datos guardados en tips_data_{timestamp}.json")

browser = None
page = None

start = 0
end = 9 #indice de cada usuario a procesar
    
try:
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=False)
        page = browser.new_page()
        page.goto('https://app.foursquare.com/login')
        page.wait_for_timeout(np.random.uniform(5000, 6000))
        page.fill('input[id="username"]', credentials['username'])
        page.fill('input[id="password"]', credentials['password'])
        page.click('input[id="loginFormButton"]')
        page.pause() # Pause for enter the 2FA code
        
        for idx, (_, info) in enumerate(urls_reseñantes.iloc[start:end+1].iterrows()):
            url = info['url_usuario']
            user_info = {'user': info['nombre_usuario'], 'user_id': url.split('/')[-1]}
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
                    continue
                
                # Select all tips within the container
                tips = tips_container.query_selector_all('.tipCard')                
                try:
                    # Loop through each tip and extract the required information
                    for tip in tips:
                        tip_info = {}
                        tip_info['user'] = info['nombre_usuario']
                        tip_info['user_id'] = url.split('/')[-1]
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
            print(f"Reseñante: {info['nombre_usuario']}, ID: {url.split('/')[-1]}, Total de tips: {len(user_tips)}")
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