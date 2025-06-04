import os
import json
from config.settings import Settings

def guardar_progreso(idx_actual, processed_user_ids):
    with open(Settings.PROGRESO_PATH, "w", encoding="utf-8") as f:
        json.dump({"idx_actual": idx_actual, "processed_user_ids": list(processed_user_ids)}, f, ensure_ascii=False, indent=4)
    print(f"Progreso guardado en {Settings.PROGRESO_PATH}")

def cargar_progreso():
    if os.path.exists(Settings.PROGRESO_PATH):
        with open(Settings.PROGRESO_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def save_log_error(info):
    """
    Guarda la información del usuario en un solo archivo error_tips.json (lista de dicts).
    """
    os.makedirs(Settings.LOGS_ERROR_DIR, exist_ok=True)
    if os.path.exists(Settings.ERROR_TIPS_PATH):
        with open(Settings.ERROR_TIPS_PATH, "r", encoding="utf-8") as f:
            usuarios = json.load(f)
    else:
        usuarios = []
    user_id = info['url_usuario'].split('/')[-1]
    if not any(u.get("user_id") == user_id for u in usuarios):
        usuarios.append({
            "nombre_usuario": info['nombre_usuario'],
            "user_id": user_id,
            "url_usuario": info['url_usuario'],
            "error": "No se encontró el botón 'Ver todos los tips'"
        })
        with open(Settings.ERROR_TIPS_PATH, "w", encoding="utf-8") as f:
            json.dump(usuarios, f, ensure_ascii=False, indent=4)
        print(f"Usuario con error agregado a: {Settings.ERROR_TIPS_PATH}")
    else:
        print(f"Usuario {user_id} ya registrado en {Settings.ERROR_TIPS_PATH}")