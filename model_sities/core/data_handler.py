"""
Gestión y almacenamiento de datos extraídos
"""
import os
import json
from typing import Dict, List, Any
from ..config.settings import Settings
from ..utils.helpers import current_timestamp

class DataHandler:
    """Maneja el almacenamiento y gestión de los datos extraídos"""
    
    def __init__(self, output_dir: str = None):
        """Inicializa el gestor de datos"""
        self.output_dir = output_dir or Settings.SITIES_OUTPUT_DIR
        self.all_sitios: Dict[str, List[Dict]] = {}
        self.processed_urls: Dict[str, Dict] = {}
        self._ensure_output_dir()
    
    def _ensure_output_dir(self) -> None:
        """Asegura que exista el directorio de salida"""
        os.makedirs(self.output_dir, exist_ok=True)
    
    def Add_sites(self, municipio: str, sites: List[Dict], processing_index: int) -> Dict[str, int]:
        """
        Añade sitios a un municipio, evitando duplicados.
        """
        if municipio not in self.all_sitios:
            self.all_sitios[municipio] = []
        
        existing_urls = {sitio.get('url_sitio', '') for sitio in self.all_sitios[municipio]}
        new_sites = []
        
        for site in sites:
            site_url = site.get('url_sitio', '')
            if site_url and site_url not in existing_urls:
                site['municipio'] = municipio
                site['indice_procesamiento'] = processing_index
                new_sites.append(site)
                existing_urls.add(site_url)
        
        self.all_sitios[municipio].extend(new_sites)
        
        for i, site in enumerate(self.all_sitios[municipio]):
            site['id'] = i + 1
        
        return {
            'new_sites': len(new_sites),
            'duplicates_omitted': len(sites) - len(new_sites),
            'total_sites': len(self.all_sitios[municipio])
        }
    
    def Update_processed_url(self, municipio: str, url: str, stats: Dict[str, Any]) -> None:
        """Actualiza la información de una URL procesada"""
        self.processed_urls[municipio] = {
            'url': url,
            'fecha_procesamiento': current_timestamp(),
            **stats
        }
    
    def Save_municipio_data(self, municipio: str) -> bool:
        """Guarda los datos de un municipio haciendo merge incremental para no perder datos previos."""
        if municipio not in self.all_sitios or not self.all_sitios[municipio]:
            return False

        archivo_municipio = os.path.join(self.output_dir, f'sitios_{municipio}.json')
        sitios_previos = []

        # Leer datos previos si existen
        if os.path.exists(archivo_municipio):
            try:
                with open(archivo_municipio, 'r', encoding='utf-8') as file:
                    datos_previos = json.load(file)
                    sitios_previos = datos_previos.get("sitios_turisticos", [])
            except Exception:
                sitios_previos = []

        # Crear set de URLs previas para evitar duplicados
        urls_previas = set(s.get("url_sitio") for s in sitios_previos if "url_sitio" in s)
        sitios_final = sitios_previos.copy()

        # Agregar solo los sitios nuevos que no estén en los previos
        for sitio in self.all_sitios[municipio]:
            if sitio.get("url_sitio") not in urls_previas:
                sitios_final.append(sitio)
                urls_previas.add(sitio.get("url_sitio"))

        datos = {
            "municipio": municipio,
            "sitios_turisticos": sitios_final,
            "total": len(sitios_final),
            "fecha_extraccion": current_timestamp(),
            "url_procesada": self.processed_urls.get(municipio, {})
        }

        try:
            with open(archivo_municipio, 'w', encoding='utf-8') as file:
                json.dump(datos, file, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"[ERROR] No se pudo guardar datos de {municipio}: {e}")
            return False
            
    def Save_all_data(self) -> bool:
        """Guarda los datos de todos los municipios procesados"""
        try:
            for municipio in self.all_sitios.keys():
                self.save_municipio_data(municipio)
            
            total_sitios = sum(len(sitios) for sitios in self.all_sitios.values())
            resumen = {
                "resumen_general": {
                    "total_municipios_procesados": len(self.all_sitios),
                    "total_sitios_extraidos": total_sitios,
                    "fecha_procesamiento": current_timestamp()
                },
                "municipios": {municipio: len(sitios) for municipio, sitios in self.all_sitios.items()},
                "urls_procesadas": self.processed_urls
            }
            
            archivo_resumen = os.path.join(self.output_dir, 'resumen_extraccion.json')
            with open(archivo_resumen, 'w', encoding='utf-8') as file:
                json.dump(resumen, file, ensure_ascii=False, indent=4)
            print(f"[INFO] Resumen general guardado en {archivo_resumen}")
            return True
        except Exception as e:
            print(f"[ERROR] No se pudo guardar el resumen general: {e}")
            return False
    
    def Get_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas actuales de los datos"""
        total_sitios = sum(len(sitios) for sitios in self.all_sitios.values())
        return {
            'total_municipalities': len(self.all_sitios),
            'total_sites': total_sitios,
            'municipalities': list(self.all_sitios.keys()),
            'sites_per_municipality': {municipio: len(sitios) for municipio, sitios in self.all_sitios.items()}
        }