import os
import unittest
from dotenv import load_dotenv

from connection_util import create_mastodon
from models.language import LANGUAGE
from models.platform import PLATFORM
from socialmedia import download_conversations, download_daily_sample_conversations
from datasource.mastodon.download_user_conversations import download_user_conversations


class DelabTreeConstructionTestCase(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        self.mst_client_id = os.environ.get("mst_client_id")
        self.mst_client_secret = os.environ.get("mst_client_secret")
        self.mst_access_token = os.environ.get("mst_access_token")
        assert self.mst_client_id is not None
        assert self.mst_client_secret is not None
        assert self.mst_access_token is not None

    def test_connection(self):
        self.mastodon = create_mastodon()
        # Fetch the public timeline
        public_posts = self.mastodon.timeline_public()

        # Print the content of the first post in the public timeline # assuming you have one
        assert public_posts

    def test_tree_download(self):
        assert self.mst_client_id is not None, "environment variable not set"
        conversations = download_conversations(query_string="Trump", platform=PLATFORM.MASTODON)
        assert len(conversations) > 0

    def test_post_message(self):
        pass

    def test_download_daily(self):
        assert self.mst_client_id is not None, "environment variable not set"
        conversations = download_daily_sample_conversations(platform=PLATFORM.MASTODON,
                                                            language=LANGUAGE.ENGLISH,
                                                            min_results=5)
        assert len(conversations) > 0

    def test_get_user_conversations(self):
        assert self.mst_client_id is not None, "environment variable not set"
        conversations = download_user_conversations("@delab_goettingen", max_conversations=5)
        assert len(conversations) > 0


if __name__ == '__main__':
    unittest.main()
