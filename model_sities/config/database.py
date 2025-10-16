"""Configuración centralizada para MongoDB Atlas con índices optimizados."""

import os
from typing import Optional
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

load_dotenv()


class MongoDBConfig:
    """Gestiona la conexión y configuración de MongoDB Atlas."""
    
    MONGODB_URI = os.getenv("MONGODB_URI")
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
    
    CONNECTION_POOL_SIZE = int(os.getenv("MONGODB_POOL_SIZE", "50"))
    CONNECTION_TIMEOUT_MS = int(os.getenv("MONGODB_TIMEOUT_MS", "10000"))
    MAX_IDLE_TIME_MS = int(os.getenv("MONGODB_MAX_IDLE_MS", "120000"))
    
    COLLECTION_SITIES = "sities"
    COLLECTION_REVIEWERS = "reviewers"
    COLLECTION_PROGRESS = "progress"
    COLLECTION_SITIES_STATS = "sities_stats"
    
    _client: Optional[MongoClient] = None
    _db = None
    
    @classmethod
    def get_client(cls) -> MongoClient:
        """Obtiene o crea el cliente de MongoDB con connection pooling."""
        if cls._client is None:
            cls._client = MongoClient(
                cls.MONGODB_URI,
                maxPoolSize=cls.CONNECTION_POOL_SIZE,
                minPoolSize=10,
                serverSelectionTimeoutMS=cls.CONNECTION_TIMEOUT_MS,
                connectTimeoutMS=cls.CONNECTION_TIMEOUT_MS,
                socketTimeoutMS=cls.CONNECTION_TIMEOUT_MS,
                maxIdleTimeMS=cls.MAX_IDLE_TIME_MS,
                retryWrites=True,
                w='majority',
                readPreference='primaryPreferred',
                appName='foursquare_scraper'
            )
            try:
                cls._client.admin.command('ping')
                print("[INFO] Conexión a MongoDB Atlas establecida.")
            except ConnectionFailure as e:
                print(f"[ERROR] No se pudo conectar a MongoDB Atlas: {e}")
                raise
        return cls._client
    
    @classmethod
    def get_database(cls):
        """Obtiene la base de datos y crea índices si no existen."""
        if cls._db is None:
            client = cls.get_client()
            cls._db = client[cls.MONGODB_DATABASE]
            cls._create_indexes()
        return cls._db
    
    @classmethod
    def _create_indexes(cls):
        """Crea índices optimizados para consultas por municipio."""
        db = cls._db
        
        db[cls.COLLECTION_SITIES].create_index(
            [("url_sitio", ASCENDING)],
            unique=True,
            name="idx_url_sitio_unique"
        )
        
        db[cls.COLLECTION_SITIES].create_index(
            [("municipio", ASCENDING), ("fecha_extraccion", DESCENDING)],
            name="idx_municipio_fecha"
        )
        
        db[cls.COLLECTION_SITIES].create_index(
            [("municipio", ASCENDING), ("categoria", ASCENDING)],
            name="idx_municipio_categoria"
        )
        
        db[cls.COLLECTION_SITIES].create_index(
            [("municipio", ASCENDING), ("puntuacion", DESCENDING)],
            name="idx_municipio_puntuacion"
        )
        
        db[cls.COLLECTION_SITIES].create_index(
            [("id", ASCENDING)],
            name="idx_id"
        )
        
        db[cls.COLLECTION_REVIEWERS].create_index(
            [("user_url", ASCENDING), ("site_id", ASCENDING)],
            unique=True,
            name="idx_user_site_unique"
        )
        db[cls.COLLECTION_REVIEWERS].create_index(
            [("municipio", ASCENDING)],
            name="idx_reviewer_municipio"
        )
        db[cls.COLLECTION_REVIEWERS].create_index(
            [("site_id", ASCENDING)],
            name="idx_reviewer_site"
        )
        
        db[cls.COLLECTION_PROGRESS].create_index(
            [("module", ASCENDING)],
            unique=True,
            name="idx_module_unique"
        )
        
        print("[INFO] Índices de MongoDB creados correctamente.")
    
    @classmethod
    def create_materialized_views(cls):
        """Crea vistas materializadas para estadísticas por municipio."""
        db = cls._db
        
        db[cls.COLLECTION_SITIES_STATS].drop()
        
        pipeline = [
            {
                "$group": {
                    "_id": "$municipio",
                    "total_sitios": {"$sum": 1},
                    "categorias_unicas": {"$addToSet": "$categoria"},
                    "puntuacion_promedio": {
                        "$avg": {"$toDouble": "$puntuacion"}
                    },
                    "ultima_actualizacion": {"$max": "$fecha_extraccion"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "municipio": "$_id",
                    "total_sitios": 1,
                    "total_categorias": {"$size": "$categorias_unicas"},
                    "categorias": "$categorias_unicas",
                    "puntuacion_promedio": {
                        "$round": ["$puntuacion_promedio", 2]
                    },
                    "ultima_actualizacion": 1
                }
            },
            {
                "$out": cls.COLLECTION_SITIES_STATS
            }
        ]
        
        db[cls.COLLECTION_SITIES].aggregate(pipeline)
        
        db[cls.COLLECTION_SITIES_STATS].create_index(
            [("municipio", ASCENDING)],
            unique=True
        )
        
        print("[INFO] Vistas materializadas creadas correctamente.")
    
    @classmethod
    def close_connection(cls):
        """Cierra la conexión a MongoDB."""
        if cls._client:
            cls._client.close()
            cls._client = None
            cls._db = None
            print("[INFO] Conexión a MongoDB cerrada.")