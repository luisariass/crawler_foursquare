"""Gestión de datos con consultas optimizadas por municipio."""

from typing import Dict, List, Any, Optional
from pymongo.errors import DuplicateKeyError

from ..config.database import MongoDBConfig
from ..utils.helpers import current_timestamp


class MongoDataHandler:
    """Manejador de datos con consultas optimizadas."""
    
    def __init__(self):
        self.db = MongoDBConfig.get_database()
        self.sities_collection = self.db[MongoDBConfig.COLLECTION_SITIES]
        self.reviewers_collection = self.db[
            MongoDBConfig.COLLECTION_REVIEWERS
        ]
        self.progress_collection = self.db[MongoDBConfig.COLLECTION_PROGRESS]
        self.stats_collection = self.db[MongoDBConfig.COLLECTION_SITIES_STATS]
    
    def load_data_sities(self):
        """Carga datos de sitios desde MongoDB."""
        total_sites = self.sities_collection.count_documents({})
        print(f"[INFO] Cargados {total_sites} sitios desde MongoDB.")
    
    def load_data_reviewers(self):
        """Carga datos de reviewers desde MongoDB."""
        total_reviewers = self.reviewers_collection.count_documents({})
        print(f"[INFO] Cargados {total_reviewers} reviewers desde MongoDB.")
    

    def add_sites(self, municipio: str, departamento: str, sites: List[Dict]) -> Dict[str, int]:  # Agregado departamento como parámetro, replicando municipio
        """Añade sitios a MongoDB evitando duplicados."""
        if not sites:
            return {
                'new_sites': 0,
                'duplicates_omitted': 0,
                'total_items': 0
            }
        
        new_count = 0
        duplicates_count = 0
        
        for site in sites:
            site['municipio'] = municipio
            site['departamento'] = departamento  # Agregado, replicando municipio
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
    
    def get_sites_by_municipio(
        self,
        municipio: str,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "fecha_extraccion",
        sort_order: int = -1
    ) -> List[Dict]:
        """Obtiene sitios de un municipio con paginación."""
        cursor = self.sities_collection.find(
            {"municipio": municipio},
            {"_id": 0}
        ).sort(sort_by, sort_order).skip(skip).limit(limit)
        
        return list(cursor)
    
    def get_sites_by_categoria(
        self,
        municipio: str,
        categoria: str
    ) -> List[Dict]:
        """Obtiene sitios filtrados por municipio y categoría."""
        cursor = self.sities_collection.find(
            {
                "municipio": municipio,
                "categoria": categoria
            },
            {"_id": 0}
        )
        
        return list(cursor)
    
    def get_top_sites_by_municipio(
        self,
        municipio: str,
        limit: int = 10
    ) -> List[Dict]:
        """Obtiene mejores sitios de un municipio por puntuación."""
        cursor = self.sities_collection.find(
            {"municipio": municipio},
            {"_id": 0}
        ).sort("puntuacion", -1).limit(limit)
        
        return list(cursor)
    
    def get_municipio_summary(self, municipio: str) -> Dict[str, Any]:
        """Obtiene resumen estadístico de un municipio."""
        stats = self.stats_collection.find_one(
            {"municipio": municipio},
            {"_id": 0}
        )
        
        if stats:
            return stats
        
        pipeline = [
            {"$match": {"municipio": municipio}},
            {
                "$group": {
                    "_id": None,
                    "total_sitios": {"$sum": 1},
                    "categorias": {"$addToSet": "$categoria"},
                    "puntuacion_promedio": {
                        "$avg": {"$toDouble": "$puntuacion"}
                    }
                }
            }
        ]
        
        result = list(self.sities_collection.aggregate(pipeline))
        
        if result:
            return {
                "municipio": municipio,
                "total_sitios": result[0]["total_sitios"],
                "total_categorias": len(result[0]["categorias"]),
                "categorias": result[0]["categorias"],
                "puntuacion_promedio": round(
                    result[0]["puntuacion_promedio"],
                    2
                )
            }
        
        return {}
    
    def get_all_municipios(self) -> List[str]:
        """Obtiene lista de todos los municipios únicos."""
        return self.sities_collection.distinct("municipio")
    
    def refresh_stats(self):
        """Refresca las estadísticas materializadas."""
        MongoDBConfig.create_materialized_views()
        print("[INFO] Estadísticas actualizadas correctamente.")
    
    def add_reviewers(
        self,
        context: Dict,
        reviewers: List[Dict]
    ) -> Dict[str, int]:
        """Añade reviewers a MongoDB evitando duplicados."""
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
    
    def save_sites_data(self, municipio: str) -> bool:
        """Método de compatibilidad."""
        return True
    
    def save_reviewers_data(self, context: Dict) -> bool:
        """Método de compatibilidad."""
        return True
    
    def save_all_data(self) -> None:
        """Método de compatibilidad."""
        print("[INFO] Datos persistidos en MongoDB Atlas.")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas generales desde vistas materializadas."""
        stats_list = list(self.stats_collection.find({}, {"_id": 0}))
        
        total_sites = sum(s.get("total_sitios", 0) for s in stats_list)
        
        return {
            'sites_stats': {
                'total_municipalities': len(stats_list),
                'total_sites': total_sites,
                'sites_per_municipality': {
                    s['municipio']: s['total_sitios'] for s in stats_list
                },
                'detailed_stats': stats_list
            },
            'reviewers_stats': self._get_reviewers_stats()
        }
    
    def _get_reviewers_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas de reviewers."""
        total_reviewers = self.reviewers_collection.count_documents({})
        
        pipeline_reviewers = [
            {"$group": {"_id": "$municipio", "count": {"$sum": 1}}}
        ]
        reviewers_by_municipio = list(
            self.reviewers_collection.aggregate(pipeline_reviewers)
        )
        
        return {
            'total_contexts': len(reviewers_by_municipio),
            'total_reviewers': total_reviewers,
            'reviewers_per_municipality': {
                item['_id']: item['count']
                for item in reviewers_by_municipio
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
    
    def load_progress(
        self,
        module: str,
        csv_path: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Carga el progreso del scraping desde MongoDB.
        
        Args:
            module: Nombre del módulo (ej: 'sities_fetcher').
            csv_path: Ruta del CSV (opcional). Si se proporciona,
                    busca el progreso específico para ese CSV.
        
        Returns:
            Documento de progreso o None si no existe.
        """
        query = {'module': module}
        
        if csv_path:
            query['csv_path'] = csv_path
        
        return self.progress_collection.find_one(
            query,
            {'_id': 0}
        )