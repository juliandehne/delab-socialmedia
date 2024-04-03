import os
import unittest


class DelabTreeConstructionTestCase(unittest.TestCase):

    def setUp(self):
        self.mst_client_id = os.environ.get("mst_client_id"),
        self.mst_client_secret = os.environ.get("mst_client_secret"),
        self.mst_access_token = os.environ.get("mst_access_token")
        assert self.mst_client_id is not None
        assert self.mst_client_secret is not None
        assert self.mst_access_token is not None

    def test_connection(self):
        pass

    def test_tree_download(self):
        pass

    def test_post_message(self):
        pass

    def test_download_daily(self):
        pass


if __name__ == '__main__':
    unittest.main()
