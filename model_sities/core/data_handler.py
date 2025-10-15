"""Gestión y almacenamiento de datos usando MongoDB Atlas."""

from typing import Dict, List, Any, Optional
from pymongo.errors import DuplicateKeyError

from ..config.database import MongoDBConfig
from ..utils.helpers import current_timestamp


class MongoDataHandler:
    """Manejador de datos que persiste en MongoDB Atlas."""
    
    def __init__(self):
        self.db = MongoDBConfig.get_database()
        self.sities_collection = self.db[MongoDBConfig.COLLECTION_SITIES]
        self.reviewers_collection = self.db[MongoDBConfig.COLLECTION_REVIEWERS]
        self.progress_collection = self.db[MongoDBConfig.COLLECTION_PROGRESS]
    
    def load_data_sities(self):
        """Carga datos de sitios desde MongoDB."""
        total_sites = self.sities_collection.count_documents({})
        print(f"[INFO] Cargados {total_sites} sitios existentes desde MongoDB.")
    
    def load_data_reviewers(self):
        """Carga datos de reviewers desde MongoDB."""
        total_reviewers = self.reviewers_collection.count_documents({})
        print(f"[INFO] Cargados {total_reviewers} reviewers existentes desde MongoDB.")
    
    def add_sites(self, municipio: str, sites: List[Dict]) -> Dict[str, int]:
        """Añade sitios a MongoDB, evitando duplicados por url_sitio."""
        if not sites:
            return {'new_sites': 0, 'duplicates_omitted': 0, 'total_items': 0}
        
        new_count = 0
        duplicates_count = 0
        
        for site in sites:
            site['municipio'] = municipio
            site['fecha_extraccion'] = current_timestamp()
            
            try:
                self.sities_collection.insert_one(site)
                new_count += 1
            except DuplicateKeyError:
                duplicates_count += 1
        
        total_in_db = self.sities_collection.count_documents(
            {'municipio': municipio}
        )
        
        return {
            'new_sites': new_count,
            'duplicates_omitted': duplicates_count,
            'total_items': total_in_db
        }
    
    def save_sites_data(self, municipio: str) -> bool:
        """Método de compatibilidad. Los datos ya están guardados."""
        return True
    
    def add_reviewers(
        self,
        context: Dict,
        reviewers: List[Dict]
    ) -> Dict[str, int]:
        """Añade reviewers a MongoDB, evitando duplicados."""
        if not reviewers:
            return {
                'new_reviewers': 0,
                'duplicates_omitted': 0,
                'total_items': 0
            }
        
        municipio = context.get("municipio", "desconocido")
        site_id = context.get("site_id", "unknown_id")
        site_name = context.get("site_name", "desconocido")
        
        new_count = 0
        duplicates_count = 0
        
        for reviewer in reviewers:
            reviewer_doc = {
                'user_name': reviewer.get('user_name'),
                'user_url': reviewer.get('user_url'),
                'site_id': site_id,
                'site_name': site_name,
                'municipio': municipio,
                'fecha_extraccion': current_timestamp()
            }
            
            try:
                self.reviewers_collection.insert_one(reviewer_doc)
                new_count += 1
            except DuplicateKeyError:
                duplicates_count += 1
        
        total_in_db = self.reviewers_collection.count_documents(
            {'site_id': site_id}
        )
        
        return {
            'new_reviewers': new_count,
            'duplicates_omitted': duplicates_count,
            'total_items': total_in_db
        }
    
    def save_reviewers_data(self, context: Dict) -> bool:
        """Método de compatibilidad. Los datos ya están guardados."""
        return True
    
    def save_all_data(self) -> None:
        """Método de compatibilidad. MongoDB guarda automáticamente."""
        print("[INFO] Datos persistidos en MongoDB Atlas.")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas desde MongoDB."""
        total_sites = self.sities_collection.count_documents({})
        
        pipeline_sites = [
            {"$group": {"_id": "$municipio", "count": {"$sum": 1}}}
        ]
        sites_by_municipio = list(
            self.sities_collection.aggregate(pipeline_sites)
        )
        
        total_reviewers = self.reviewers_collection.count_documents({})
        
        pipeline_reviewers = [
            {"$group": {"_id": "$municipio", "count": {"$sum": 1}}}
        ]
        reviewers_by_municipio = list(
            self.reviewers_collection.aggregate(pipeline_reviewers)
        )
        
        return {
            'sites_stats': {
                'total_municipalities': len(sites_by_municipio),
                'total_sites': total_sites,
                'sites_per_municipality': {
                    item['_id']: item['count'] for item in sites_by_municipio
                }
            },
            'reviewers_stats': {
                'total_contexts': len(reviewers_by_municipio),
                'total_reviewers': total_reviewers,
                'reviewers_per_municipality': {
                    item['_id']: item['count']
                    for item in reviewers_by_municipio
                }
            }
        }
    
    def get_all_sites_for_reviewers(self) -> List[Dict]:
        """Obtiene todos los sitios para procesarlos en reviewers."""
        sites = list(self.sities_collection.find(
            {},
            {
                '_id': 0,
                'id': 1,
                'nombre': 1,
                'url_sitio': 1,
                'municipio': 1
            }
        ))
        return sites
    
    def save_progress(
        self,
        module: str,
        csv_path: str,
        idx_actual: int
    ) -> None:
        """Guarda el progreso del scraping en MongoDB."""
        progress_doc = {
            'module': module,
            'csv_path': csv_path,
            'idx_actual': idx_actual,
            'timestamp': current_timestamp()
        }
        
        self.progress_collection.update_one(
            {'module': module},
            {'$set': progress_doc},
            upsert=True
        )
    
    def load_progress(self, module: str) -> Optional[Dict]:
        """Carga el progreso del scraping desde MongoDB."""
        return self.progress_collection.find_one(
            {'module': module},
            {'_id': 0}
        )