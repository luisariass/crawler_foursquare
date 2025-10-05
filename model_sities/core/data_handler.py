"""
Gestión y almacenamiento de datos extraídos usando un patrón de diseño Strategy
para manejar diferentes tipos de datos (sitios, usuarios, etc.) de forma genérica.
"""

import os
import json
from abc import ABC, abstractmethod
from typing import Dict, List, Any, TypeVar

from ..config.settings import Settings
from ..utils.helpers import current_timestamp

T = TypeVar('T', bound=Dict[str, Any])


class DataStorageStrategy(ABC):
    """
    Interfaz abstracta para definir la estrategia de almacenamiento.
    Define cómo manejar los detalles específicos de un tipo de dato.
    """

    @abstractmethod
    def get_unique_identifier(self, item: T) -> str:
        """Devuelve el identificador único para un item (ej: 'url_sitio')."""
        pass

    @abstractmethod
    def get_data_key(self) -> str:
        """Devuelve la clave principal de los datos en el JSON."""
        pass

    @abstractmethod
    def build_filepath(self, output_dir: str, context: Any) -> str:
        """
        Construye la ruta completa del archivo de salida basado en el contexto.
        """
        pass


class SitesStorageStrategy(DataStorageStrategy):
    """Estrategia para guardar sitios por municipio: sitios_<municipio>.json."""

    def get_unique_identifier(self, item: T) -> str:
        return item.get('url_sitio', '')

    def get_data_key(self) -> str:
        return 'sitios_turisticos'

    def build_filepath(self, output_dir: str, context: str) -> str:
        # El contexto es el nombre del municipio.
        return os.path.join(output_dir, f'sitios_{context}.json')


class ReviewersStorageStrategy(DataStorageStrategy):
    """
    Estrategia para guardar reseñantes por municipio/sitio:
    <municipio>/reviewers_sitio_<id>_<nombre>.json
    """

    def get_unique_identifier(self, item: T) -> str:
        return item.get('user_url', '')

    def get_data_key(self) -> str:
        return 'user_profile'

    def build_filepath(self, output_dir: str, context: Dict[str, str]) -> str:
        """
        Construye la ruta anidada. El contexto debe ser un diccionario.
        """
        municipality = context.get("municipio", "desconocido")
        site_id = context.get("site_id", "unknown_id")
        site_name = context.get("site_name", "unknown_name")

        municipio_dir = os.path.join(output_dir, municipality)
        os.makedirs(municipio_dir, exist_ok=True)

        safe_site_name = "".join(
            c for c in site_name if c.isalnum() or c in (' ', '_')
        ).rstrip().replace(' ', '_')
        filename = f"reviewers_sitio_{site_id}_{safe_site_name}.json"
        return os.path.join(municipio_dir, filename)


class GenericDataHandler:
    """
    Clase genérica que maneja la lógica común de almacenamiento de datos.
    Utiliza una estrategia para los detalles específicos.
    """

    def __init__(self, strategy: DataStorageStrategy, output_dir: str):
        self.strategy = strategy
        self.output_dir = output_dir
        self.data_by_context: Dict[str, Dict[str, Any]] = {}
        os.makedirs(self.output_dir, exist_ok=True)

    def load_all_data(self):
        """
        Carga todos los datos existentes desde el directorio de salida a la memoria.
        Recorre recursivamente el directorio buscando archivos .json.
        """
        print(f"[INFO] Buscando datos existentes en: {self.output_dir}")
        loaded_count = 0
        for root, _, files in os.walk(self.output_dir):
            for filename in files:
                if filename.endswith('.json'):
                    file_path = os.path.join(root, filename)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)

                        context = data.get('context')
                        items = data.get(self.strategy.get_data_key(), [])

                        if context and items:
                            context_key = str(context)
                            self.data_by_context[context_key] = {
                                "context_obj": context,
                                "items": items
                            }
                            loaded_count += len(items)
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"[WARN] Omitiendo archivo mal formado {file_path}: {e}")
        print(f"[INFO] Se cargaron {loaded_count} items preexistentes para la estrategia.")

    def add_items(self, context: Any, items: List[T]) -> Dict[str, int]:
        """Añade items a un contexto, evitando duplicados."""
        context_key = str(context)
        if context_key not in self.data_by_context:
            self.data_by_context[context_key] = {
                "context_obj": context,
                "items": []
            }

        existing_ids = {self.strategy.get_unique_identifier(item)
                        for item in self.data_by_context[context_key]["items"]}
        new_items = []
        for item in items:
            identifier = self.strategy.get_unique_identifier(item)
            if identifier and identifier not in existing_ids:
                new_items.append(item)
                existing_ids.add(identifier)

        self.data_by_context[context_key]["items"].extend(new_items)
        return {
            'new_items': len(new_items),
            'duplicates_omitted': len(items) - len(new_items),
            'total_items': len(self.data_by_context[context_key]["items"])
        }

    def save_context_data(self, context_key: str) -> bool:
        """Guarda datos usando la clave de contexto para encontrar el objeto original."""
        context_data = self.data_by_context.get(context_key)

        if not context_data or not context_data.get("items"):
            return False

        context_obj = context_data["context_obj"]
        items_in_memory = context_data["items"]
        file_path = self.strategy.build_filepath(self.output_dir, context_obj)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        data_to_save = {
            "municipality": context_obj if isinstance(context_obj, str) else context_obj.get("municipio"),
            "context": context_obj,
            self.strategy.get_data_key(): items_in_memory,
            "total": len(items_in_memory),
            "fecha_extraccion": current_timestamp(),
        }
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=4)
            return True
        except IOError as e:
            print(f"[ERROR] No se pudo guardar datos para {context_obj}: {e}")
            return False


class DataHandler:
    """
    Clase Facade que proporciona una interfaz simple y unificada para
    manejar diferentes tipos de datos.
    """

    def __init__(self, settings: Settings = None):
        if settings is None:
            settings = Settings()
        self.sites = GenericDataHandler(
            strategy=SitesStorageStrategy(),
            output_dir=settings.SITIES_OUTPUT_DIR
        )
        self.reviewers = GenericDataHandler(
            strategy=ReviewersStorageStrategy(),
            output_dir=settings.REVIEWS_OUTPUT_DIR
        )

    def load_all_data(self):
        """Carga todos los datos para todas las estrategias."""
        print("[INFO] Iniciando carga de todos los datos existentes...")
        self.sites.load_all_data()
        self.reviewers.load_all_data()
        print("[INFO] Carga de datos finalizada.")

    def add_sites(self, municipio: str, sites: List[Dict]) -> Dict[str, int]:
        stats = self.sites.add_items(municipio, sites)
        return {'new_sites': stats['new_items'], **stats}

    def save_sites_data(self, municipio: str) -> bool:
        return self.sites.save_context_data(str(municipio))

    def add_reviewers(self, context: Dict, reviewers: List[Dict]) -> Dict[str, int]:
        stats = self.reviewers.add_items(context, reviewers)
        return {'new_reviewers': stats['new_items'], **stats}

    def save_reviewers_data(self, context: Dict) -> bool:
        return self.reviewers.save_context_data(str(context))

    def save_all_data(self) -> None:
        print("[INFO] Guardando todos los datos de sitios pendientes...")
        contexts_to_save = list(self.sites.data_by_context.keys())
        for context_key in contexts_to_save:
            self.sites.save_context_data(context_key)

        print("[INFO] Guardando todos los datos de usuarios pendientes...")
        contexts_to_save = list(self.reviewers.data_by_context.keys())
        for context_key in contexts_to_save:
            self.reviewers.save_context_data(context_key)

    def get_statistics(self) -> Dict[str, Any]:
        sites_data = self.sites.data_by_context
        total_sitios = sum(len(v['items']) for v in sites_data.values())

        reviewers_data = self.reviewers.data_by_context
        total_usuarios = sum(len(v['items']) for v in reviewers_data.values())

        return {
            'sites_stats': {
                'total_municipalities': len(sites_data),
                'total_sites': total_sitios,
                'municipalities': [v['context_obj'] for v in sites_data.values()],
                'sites_per_municipality': {
                    k: len(v['items']) for k, v in sites_data.items()
                }
            },
            'users_stats': {
                'total_contexts': len(reviewers_data),
                'total_users': total_usuarios,
                'contexts': [v['context_obj'] for v in reviewers_data.values()],
                'users_per_context': {
                    k: len(v['items']) for k, v in reviewers_data.items()
                }
            }
        }