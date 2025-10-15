import os
import unittest
from typing import Any, Dict, List
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

load_dotenv()


class TestMongoDBIntegration(unittest.TestCase):
    """Pruebas de integración para operaciones básicas en MongoDB."""

    @classmethod
    def setUpClass(cls) -> None:
        """Configura la conexión a la base de datos antes de las pruebas."""
        mongo_uri = os.getenv("MONGODB_URI")
        cls.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        cls.db = cls.client["test_db"]
        cls.collection: Collection = cls.db["test_collection"]

    @classmethod
    def tearDownClass(cls) -> None:
        """Cierra la conexión a la base de datos después de las pruebas."""
        cls.client.close()

    def setUp(self) -> None:
        """Limpia la colección antes de cada prueba."""
        self.collection.delete_many({})

    def test_connection(self) -> None:
        """Verifica que la conexión a MongoDB sea exitosa."""
        try:
            self.client.admin.command("ping")
            connected = True
        except ConnectionFailure:
            connected = False
        self.assertTrue(connected, "No se pudo conectar a MongoDB.")

    def test_insert_document(self) -> None:
        """Prueba la inserción de un documento en la colección."""
        doc: Dict[str, Any] = {"nombre": "Sitio A", "categoria": "Museo"}
        result = self.collection.insert_one(doc)
        self.assertIsNotNone(result.inserted_id)
        self.assertEqual(self.collection.count_documents({}), 1)

    def test_find_document(self) -> None:
        """Prueba la búsqueda de un documento en la colección."""
        doc: Dict[str, Any] = {"nombre": "Sitio B", "categoria": "Parque"}
        self.collection.insert_one(doc)
        found = self.collection.find_one({"nombre": "Sitio B"})
        self.assertIsNotNone(found)
        self.assertEqual(found["categoria"], "Parque")

    def test_update_document(self) -> None:
        """Prueba la actualización de un documento en la colección."""
        doc: Dict[str, Any] = {"nombre": "Sitio C", "categoria": "Plaza"}
        self.collection.insert_one(doc)
        result = self.collection.update_one(
            {"nombre": "Sitio C"}, {"$set": {"categoria": "Monumento"}}
        )
        self.assertEqual(result.modified_count, 1)
        updated = self.collection.find_one({"nombre": "Sitio C"})
        self.assertEqual(updated["categoria"], "Monumento")

    def test_delete_document(self) -> None:
        """Prueba la eliminación de un documento en la colección."""
        doc: Dict[str, Any] = {"nombre": "Sitio D", "categoria": "Teatro"}
        self.collection.insert_one(doc)
        result = self.collection.delete_one({"nombre": "Sitio D"})
        self.assertEqual(result.deleted_count, 1)
        self.assertIsNone(self.collection.find_one({"nombre": "Sitio D"}))

    def test_insert_many_documents(self) -> None:
        """Prueba la inserción de múltiples documentos."""
        docs: List[Dict[str, Any]] = [
            {"nombre": "Sitio E", "categoria": "Museo"},
            {"nombre": "Sitio F", "categoria": "Parque"},
        ]
        result = self.collection.insert_many(docs)
        self.assertEqual(len(result.inserted_ids), 2)
        self.assertEqual(self.collection.count_documents({}), 2)


if __name__ == "__main__":
    unittest.main()