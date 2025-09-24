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
    def get_file_prefix(self) -> str:
        """Devuelve el prefijo para el nombre del archivo (ej: 'sitios')."""
        pass

    @abstractmethod
    def get_data_key(self) -> str:
        """Devuelve la clave principal de los datos en el JSON (ej: 'sitios_turisticos')."""
        pass


class SitesStorageStrategy(DataStorageStrategy):
    """Estrategia concreta para manejar los datos de 'sitios'."""

    def get_unique_identifier(self, item: T) -> str:
        return item.get('url_sitio', '')

    def get_file_prefix(self) -> str:
        return 'sitios'

    def get_data_key(self) -> str:
        return 'sitios_turisticos'


class UsersStorageStrategy(DataStorageStrategy):
    """Estrategia concreta para manejar los datos de 'usuarios'."""

    def get_unique_identifier(self, item: T) -> str:
        return item.get('user_url', '')

    def get_file_prefix(self) -> str:
        return 'usuarios'

    def get_data_key(self) -> str:
        return 'perfiles_usuarios'


class GenericDataHandler:
    """
    Clase genérica que maneja la lógica común de almacenamiento de datos.
    Utiliza una estrategia para los detalles específicos.
    """

    def __init__(self, strategy: DataStorageStrategy, output_dir: str):
        self.strategy = strategy
        self.output_dir = output_dir
        self.data_by_context: Dict[str, List[T]] = {}
        self.processed_contexts: Dict[str, Dict] = {}
        os.makedirs(self.output_dir, exist_ok=True)

    def add_items(self, context: str, items: List[T]) -> Dict[str, int]:
        """
        Añade una lista de items a un contexto, evitando duplicados,
        y devuelve estadísticas de la operación.
        """
        if context not in self.data_by_context:
            self.data_by_context[context] = []

        existing_ids = {self.strategy.get_unique_identifier(item)
                        for item in self.data_by_context[context]}
        new_items = []

        for item in items:
            identifier = self.strategy.get_unique_identifier(item)
            if identifier and identifier not in existing_ids:
                new_items.append(item)
                existing_ids.add(identifier)

        self.data_by_context[context].extend(new_items)

        return {
            'new_items': len(new_items),
            'duplicates_omitted': len(items) - len(new_items),
            'total_items': len(self.data_by_context[context])
        }

    def update_processed_context(self, context: str, url: str, stats: Dict) -> None:
        """Actualiza el estado de un contexto procesado."""
        self.processed_contexts[context] = {
            'url': url,
            'fecha_procesamiento': current_timestamp(),
            **stats
        }

    def save_context_data(self, context: str) -> bool:
        """
        Guarda los datos de un contexto específico haciendo un merge incremental.
        """
        items_in_memory = self.data_by_context.get(context, [])
        if not items_in_memory:
            return False

        file_path = os.path.join(
            self.output_dir, f'{self.strategy.get_file_prefix()}_{context}.json'
        )

        previous_items = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    previous_data = json.load(f)
                    previous_items = previous_data.get(self.strategy.get_data_key(), [])
            except (json.JSONDecodeError, IOError):
                print(f"[WARN] No se pudo leer el archivo existente: {file_path}")

        existing_ids = {self.strategy.get_unique_identifier(item)
                        for item in previous_items}
        final_items = previous_items.copy()

        for item in items_in_memory:
            identifier = self.strategy.get_unique_identifier(item)
            if identifier and identifier not in existing_ids:
                final_items.append(item)
                existing_ids.add(identifier)

        data_to_save = {
            "contexto": context,
            self.strategy.get_data_key(): final_items,
            "total": len(final_items),
            "fecha_extraccion": current_timestamp(),
            "contexto_procesado": self.processed_contexts.get(context, {})
        }

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data_to_save, f, ensure_ascii=False, indent=4)
            return True
        except IOError as e:
            print(f"[ERROR] No se pudo guardar datos para {context}: {e}")
            return False


class DataHandler:
    """
    Clase Facade que proporciona una interfaz simple para manejar
    diferentes tipos de datos (sitios, usuarios, etc.).
    """

    def __init__(self, settings: Settings = None):
        if settings is None:
            settings = Settings()

        self.sites = GenericDataHandler(
            strategy=SitesStorageStrategy(),
            output_dir=settings.SITIES_OUTPUT_DIR
        )

        self.users = GenericDataHandler(
            strategy=UsersStorageStrategy(),
            output_dir=settings.REVIEWS_OUTPUT_DIR
        )

    def add_sites(self, municipio: str, sites: List[Dict]) -> Dict[str, int]:
        stats = self.sites.add_items(municipio, sites)
        return {
            'new_sites': stats['new_items'],
            'duplicates_omitted': stats['duplicates_omitted'],
            'total_sites': stats['total_items']
        }

    def update_processed_url(self, municipio: str, url: str, stats: Dict) -> None:
        self.sites.update_processed_context(municipio, url, stats)

    def save_municipio_data(self, municipio: str) -> bool:
        return self.sites.save_context_data(municipio)

    def add_users(self, context: str, users: List[Dict]) -> Dict[str, int]:
        stats = self.users.add_items(context, users)
        return {
            'new_users': stats['new_items'],
            'duplicates_omitted': stats['duplicates_omitted'],
            'total_users': stats['total_items']
        }

    def update_processed_user_context(self, context: str, url: str, stats: Dict) -> None:
        self.users.update_processed_context(context, url, stats)

    def save_user_data(self, context: str) -> list:
        return self.users.save_context_data(context)

    def save_all_data(self) -> None:
        print("[INFO] Guardando todos los datos de sitios...")
        for context in self.sites.data_by_context.keys():
            self.sites.save_context_data(context)

        print("[INFO] Guardando todos los datos de usuarios...")
        for context in self.users.data_by_context.keys():
            self.users.save_context_data(context)

    def get_statistics(self) -> Dict[str, Any]:
        sites_data = self.sites.data_by_context
        total_sitios = sum(len(sitios) for sitios in sites_data.values())

        users_data = self.users.data_by_context
        total_usuarios = sum(len(usuarios) for usuarios in users_data.values())

        return {
            'sites_stats': {
                'total_municipalities': len(sites_data),
                'total_sites': total_sitios,
                'municipalities': list(sites_data.keys()),
                'sites_per_municipality': {
                    municipio: len(sitios) for municipio, sitios in sites_data.items()
                }
            },
            'users_stats': {
                'total_contexts': len(users_data),
                'total_users': total_usuarios,
                'contexts': list(users_data.keys()),
                'users_per_context': {
                    context: len(usuarios) for context, usuarios in users_data.items()
                }
            }
        }