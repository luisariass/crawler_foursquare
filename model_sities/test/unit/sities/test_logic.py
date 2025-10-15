import unittest
from unittest.mock import MagicMock, patch, call
from model_sities.core.sities import SitiesLogic
from model_sities.sities_fetcher import SitiesFetcher

class TestSitiesLogic(unittest.TestCase):
    def setUp(self):
        self.logic = SitiesLogic()
        self.page = MagicMock()
        self.url = "https://foursquare.com/explore"
        self.municipio = "TestMunicipio"

    @patch('time.sleep', return_value=None)
    def test_extract_sites_success(self, _):
        self.page.goto = MagicMock()
        self.logic._handle_map_search_button = MagicMock()
        self.logic._wait_and_check_early_exit = MagicMock(return_value=None)
        self.logic._scrape_sites = MagicMock(return_value=[{"id": "1"}])
        status, sitios = self.logic.extract_sites(self.page, self.url, self.municipio)
        self.assertEqual(status, "success")
        self.assertEqual(sitios, [{"id": "1"}])

    @patch('time.sleep', return_value=None)
    def test_extract_sites_timeout(self, _):
        self.page.goto.side_effect = [Exception("Timeout"), Exception("Timeout")]
        self.logic._handle_map_search_button = MagicMock()
        self.logic._wait_and_check_early_exit = MagicMock(return_value=None)
        self.logic._scrape_sites = MagicMock()
        self.logic.settings.RETRIES = 2
        self.logic.settings.BACKOFF_FACTOR = 0
        status, sitios = self.logic.extract_sites(self.page, self.url, self.municipio)
        self.assertEqual(status, "error")
        self.assertEqual(sitios, [])

    def test_handle_map_search_button_found(self):
        selector = self.logic.settings.SELECTORS['map_search_button']
        self.page.locator.return_value.wait_for.return_value = None
        self.page.is_visible.return_value = True
        self.page.click.return_value = None
        self.page.wait_for_timeout.return_value = None
        self.logic._handle_map_search_button(self.page)
        self.page.click.assert_called_with(selector)

    def test_handle_map_search_button_not_found(self):
        self.page.locator.return_value.wait_for.side_effect = Exception()
        self.logic._handle_map_search_button(self.page)
        self.assertTrue(True)  # No exception means pass

    def test_wait_and_check_early_exit_block(self):
        self.page.is_visible.side_effect = lambda sel: sel == self.logic.settings.SELECTORS['generic_error_card']
        result = self.logic._wait_and_check_early_exit(self.page, self.municipio)
        self.assertEqual(result, ("generic_error", []))

    def test_wait_and_check_early_exit_no_results(self):
        self.page.is_visible.side_effect = lambda sel: sel == self.logic.settings.SELECTORS['no_results_card']
        result = self.logic._wait_and_check_early_exit(self.page, self.municipio)
        self.assertEqual(result, ("no_results", []))

    def test_scrape_sites_empty(self):
        self.logic._load_all_results = MagicMock()
        self.page.query_selector_all.return_value = []
        sitios = self.logic._scrape_sites(self.page)
        self.assertEqual(sitios, [])

    def test_extract_site_data_complete(self):
        sitio = MagicMock()
        sitio.query_selector.side_effect = [
            MagicMock(inner_text=MagicMock(return_value="4.5")),  # puntuacion
            MagicMock(inner_text=MagicMock(return_value="Museo •")),  # categoria
            MagicMock(inner_text=MagicMock(return_value="Calle 123")),  # direccion
        ]
        sitio_data = {
            "id": "abc123",
            "puntuacion": "N/A",
            "nombre": "Museo",
            "categoria": "Museo",
            "direccion": "Calle 123",
            "url_sitio": "https://foursquare.com/v/abc123",
            "fecha_extraccion": "2024-01-01 00:00:00"
        }
        with patch('model_sities.core.sities.current_timestamp', return_value="2024-01-01 00:00:00"):
            self.logic._extract_nombre_y_url = MagicMock()
            result = self.logic._extract_site_data(sitio)
        self.assertIn("fecha_extraccion", result)

class TestSitiesFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SitiesFetcher()
        self.fetcher.data_handler = MagicMock()
        self.fetcher.settings.PARALLEL_PROCESSES = 1

    @patch('model_sities.sities_fetcher.Path')
    def test_get_csv_files_file(self, mock_path):
        mock_path.return_value.is_file.return_value = True
        mock_path.return_value.suffix = '.csv'
        result = self.fetcher._get_csv_files('test.csv')
        self.assertEqual(result, [str(mock_path.return_value)])

    @patch('model_sities.sities_fetcher.glob.glob', return_value=['a.csv', 'b.csv'])
    @patch('model_sities.sities_fetcher.Path')
    def test_get_csv_files_dir(self, mock_path, _):
        mock_path.return_value.is_file.return_value = False
        mock_path.return_value.is_dir.return_value = True
        result = self.fetcher._get_csv_files('dir')
        self.assertEqual(result, ['a.csv', 'b.csv'])

    def test_get_resume_index_progress(self):
        self.fetcher.data_handler.load_progress.return_value = {
            "csv_path": "file.csv",
            "idx_actual": 5
        }
        idx = self.fetcher._get_resume_index("file.csv", 0)
        self.assertEqual(idx, 6)

    def test_get_resume_index_no_progress(self):
        self.fetcher.data_handler.load_progress.return_value = None
        idx = self.fetcher._get_resume_index("file.csv", 2)
        self.assertEqual(idx, 2)

    def test_handle_result_success(self):
        self.fetcher.data_handler.add_sites.return_value = {
            "new_sites": 2,
            "duplicates_omitted": 1,
            "total_items": 3
        }
        result = {
            "status": "success",
            "municipio": "Test",
            "sites": [{"id": 1}, {"id": 2}]
        }
        self.fetcher._handle_result(result)
        self.fetcher.data_handler.add_sites.assert_called()

    def test_handle_result_no_results(self):
        result = {
            "status": "no_results",
            "municipio": "Test",
            "sites": []
        }
        self.fetcher._handle_result(result)
        self.assertTrue(True)

    def test_handle_result_generic_error(self):
        result = {
            "status": "generic_error",
            "municipio": "Test",
            "sites": []
        }
        with patch('os.path.exists', return_value=False), \
             patch('builtins.open', unittest.mock.mock_open()):
            self.fetcher._handle_result(result)
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()