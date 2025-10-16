import os
import unittest
from typing import Dict, List, Any
from model_sities.core.data_handler import MongoDataHandler
from model_sities.config.database import MongoDBConfig

"""Tests de integración para MongoDataHandler con MongoDB Atlas."""



TEST_MUNICIPIO = "test_municipio"
TEST_SITE_ID = "test_site_id"
TEST_SITE_NAME = "Test Site"
TEST_USER_URL = "https://foursquare.com/user/test"
TEST_USER_NAME = "test_user"


class TestMongoDataHandlerIntegration(unittest.TestCase):
    """Pruebas de integración para MongoDataHandler."""

    @classmethod
    def setUpClass(cls):
        """Inicializa conexión y limpia datos de test."""
        cls.handler = MongoDataHandler()
        cls.db = MongoDBConfig.get_database()
        cls.sities = cls.db[MongoDBConfig.COLLECTION_SITIES]
        cls.reviewers = cls.db[MongoDBConfig.COLLECTION_REVIEWERS]
        cls.progress = cls.db[MongoDBConfig.COLLECTION_PROGRESS]

        # Limpia datos previos de test
        cls.sities.delete_many({'municipio': TEST_MUNICIPIO})
        cls.reviewers.delete_many({'municipio': TEST_MUNICIPIO})
        cls.progress.delete_many({'module': 'test_module'})

    @classmethod
    def tearDownClass(cls):
        """Limpia datos de test y cierra conexión."""
        cls.sities.delete_many({'municipio': TEST_MUNICIPIO})
        cls.reviewers.delete_many({'municipio': TEST_MUNICIPIO})
        cls.progress.delete_many({'module': 'test_module'})
        MongoDBConfig.close_connection()

    def test_add_sites_and_duplicates(self):
        """Verifica inserción y omisión de duplicados en sitios."""
        sites = [
            {
                'id': 'site1',
                'nombre': 'Sitio Uno',
                'url_sitio': 'https://foursquare.com/v/site1',
                'categoria': 'Parque',
                'puntuacion': '4.5'
            },
            {
                'id': 'site2',
                'nombre': 'Sitio Dos',
                'url_sitio': 'https://foursquare.com/v/site2',
                'categoria': 'Museo',
                'puntuacion': '4.0'
            }
        ]
        result = self.handler.add_sites(TEST_MUNICIPIO, sites)
        self.assertEqual(result['new_sites'], 2)
        self.assertEqual(result['duplicates_omitted'], 0)

        # Intentar insertar los mismos sitios (deberían ser duplicados)
        result_dup = self.handler.add_sites(TEST_MUNICIPIO, sites)
        self.assertEqual(result_dup['new_sites'], 0)
        self.assertEqual(result_dup['duplicates_omitted'], 2)

    def test_add_reviewers_and_duplicates(self):
        """Verifica inserción y omisión de duplicados en reviewers."""
        reviewers = [
            {
                'user_name': TEST_USER_NAME,
                'user_url': TEST_USER_URL
            }
        ]
        context = {
            'municipio': TEST_MUNICIPIO,
            'site_id': TEST_SITE_ID,
            'site_name': TEST_SITE_NAME
        }
        result = self.handler.add_reviewers(context, reviewers)
        self.assertEqual(result['new_reviewers'], 1)
        self.assertEqual(result['duplicates_omitted'], 0)

        # Intentar insertar el mismo reviewer (debería ser duplicado)
        result_dup = self.handler.add_reviewers(context, reviewers)
        self.assertEqual(result_dup['new_reviewers'], 0)
        self.assertEqual(result_dup['duplicates_omitted'], 1)

    def test_save_and_load_progress(self):
        """Verifica guardar y cargar progreso."""
        module = 'test_module'
        csv_path = '/tmp/test.csv'
        idx_actual = 42

        self.handler.save_progress(module, csv_path, idx_actual)
        progress = self.handler.load_progress(module)
        self.assertIsNotNone(progress)
        self.assertEqual(progress['module'], module)
        self.assertEqual(progress['csv_path'], csv_path)
        self.assertEqual(progress['idx_actual'], idx_actual)

    def test_get_statistics(self):
        """Verifica obtención de estadísticas."""
        stats = self.handler.get_statistics()
        self.assertIn('sites_stats', stats)
        self.assertIn('reviewers_stats', stats)
        self.assertIsInstance(stats['sites_stats']['sites_per_municipality'], dict)
        self.assertIsInstance(stats['reviewers_stats']['reviewers_per_municipality'], dict)

    def test_get_all_sites_for_reviewers(self):
        """Verifica obtención de todos los sitios para reviewers."""
        sites = self.handler.get_all_sites_for_reviewers()
        self.assertIsInstance(sites, list)
        for site in sites:
            self.assertIn('id', site)
            self.assertIn('nombre', site)
            self.assertIn('url_sitio', site)
            self.assertIn('municipio', site)


if __name__ == "__main__":
    unittest.main()