import json

# Leer el primer archivo
with open('C:\\Users\\luisarias\\Documents\\proyecto_scrapping\\tips_incomplete.json', 'r', encoding='utf-8') as f:
    tips_incomplete = json.load(f)

# Leer el segundo archivo
with open('C:\\Users\\luisarias\\Documents\\proyecto_scrapping\\model_users\\resultados\\tips_incomplete2.json', 'r', encoding='utf-8') as f:
    tips_incomplete2 = json.load(f)

with open('C:\\Users\\luisarias\\Documents\\proyecto_scrapping\\model_users\\resultados\\tips_incomplete4.json', 'r', encoding='utf-8') as f:
    tips_incomplete4 = json.load(f)

with open('C:\\Users\\luisarias\\Documents\\proyecto_scrapping\\resultados\\tips_incomplete3.json', 'r', encoding='utf-8') as f:
    tips_incomplete3 = json.load(f)

# Unir las listas
combined_tips = tips_incomplete + tips_incomplete2 + tips_incomplete3 + tips_incomplete4

# Guardar el resultado en un nuevo archivo
with open('model_users/resultados/tips_atlantico_bolivar.json', 'w', encoding='utf-8') as f:
    json.dump(combined_tips, f, ensure_ascii=False, indent=2)
