# Descripción General del Flujo de la Herramienta de Scraping

Este proyecto está organizado en **fases** que reflejan la evolución y modularización del proceso de scraping y análisis de datos turísticos y de usuarios en Foursquare. Cada fase corresponde a un módulo y una lógica específica, facilitando la escalabilidad y el mantenimiento.

---

## Fase 0: Extracción y Organización de Municipios (caribbean_grid)

- **Objetivo:** Obtener y organizar la información oficial de municipios de los departamentos del Caribe colombiano.
- **Flujo:**
  1. Se consulta una API oficial para obtener los municipios de cada departamento.
  2. Los datos se procesan y normalizan.
  3. Se generan archivos CSV por departamento en `caribbean_grid/data/.csv/`.
- **Resultado:** Archivos CSV listos para ser usados en fases posteriores.

---

## Fase 1: Extracción de Sitios Turísticos (model_sities)

- **Objetivo:** Scraping de sitios turísticos de Foursquare para cada municipio.
- **Flujo:**
  1. **Orquestación:** El archivo [`main.py`](model_sities/main.py) coordina el proceso.
  2. **Scraping:** Toda la lógica de scraping está en [`core/scraper.py`](model_sities/core/scraper.py):
     - Carga las URLs de los municipios desde los CSV generados en la fase 0.
     - Utiliza Playwright para navegar y extraer información de sitios turísticos.
     - Los datos extraídos se almacenan y gestionan con la clase `DataHandler`.
     - Se guardan archivos JSON por municipio con los sitios turísticos encontrados.
  3. **Persistencia:** Se guarda el progreso y se generan resúmenes de extracción.

---

## Fase 2: Extracción de Reseñas de Sitios Turísticos (model_sities)

- **Objetivo:** Extraer reseñas de los sitios turísticos identificados en la fase anterior.
- **Flujo:**
  1. **Orquestación:** El proceso se inicia desde [`extract_reviews.py`](model_sities/extract_reviews.py), que utiliza la clase `FoursquareReviewsExtractor` de [`core/reviews.py`](model_sities/core/reviews.py).
  2. **Extracción:** 
     - Se leen las URLs de los sitios turísticos desde los archivos generados en la fase 1.
     - Se navega a cada sitio y se extraen las reseñas disponibles.
     - Se maneja el progreso y posibles bloqueos de la plataforma.
  3. **Almacenamiento:** Las reseñas se guardan en archivos JSON organizados por municipio y sitio.

---

## Fase 3: Extracción de Reseñas de Usuarios (model_users)

- **Objetivo:** Extraer y organizar las reseñas publicadas por usuarios específicos en Foursquare.
- **Flujo:**
  1. **Orquestación:** El proceso se inicia desde [`extract_user_reviews.py`](model_users/extract_user_reviews.py), que utiliza la clase `UserReviewsExtractor` de [`core/user_reviews.py`](model_users/core/user_reviews.py).
  2. **Extracción:**
     - Se leen los usuarios desde un archivo CSV.
     - Se navega al perfil de cada usuario y se extraen todas sus reseñas (tips).
     - Se maneja la autenticación, el progreso y los errores.
  3. **Almacenamiento:** Las reseñas y la información de usuarios se guardan en archivos JSON, organizados en carpetas específicas.

---

## Resumen del Flujo Completo

1. **Fase 0:** Obtención y organización de municipios (caribbean_grid).
2. **Fase 1:** Scraping de sitios turísticos por municipio (model_sities).
3. **Fase 2:** Extracción de reseñas de cada sitio turístico (model_sities).
4. **Fase 3:** Extracción de reseñas de usuarios (model_users).

Cada fase depende de los datos generados en la anterior, permitiendo un flujo de trabajo robusto, modular y escalable.

---