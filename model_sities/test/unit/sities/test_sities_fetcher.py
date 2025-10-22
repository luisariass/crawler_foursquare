import unittest
from unittest.mock import MagicMock, patch
from model_sities.sities_fetcher import SitiesFetcher

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

if __name__ == "__main__":
    unittest.main()