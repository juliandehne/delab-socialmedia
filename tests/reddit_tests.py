import os
import unittest

from dotenv import load_dotenv

import api_settings
from connection_util import get_praw
from models.language import LANGUAGE
from models.platform import PLATFORM
from socialmedia import download_conversations, download_daily_sample_conversations
from datasource.reddit.download_user_conversations import get_user_conversations


class DelabTreeConstructionTestCase(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        self.reddit_secret = os.environ.get("reddit_secret")
        assert self.reddit_secret is not None

    def test_connection(self):
        try:
            self.connector = get_praw()
            subreddit = self.connector.subreddit('test')
            for post in subreddit.new(limit=10):
                print(f"Post ID: {post.id} - Title: {post.title}")
        except Exception as e:
            print(f"Failed to connect to Reddit: {e}")
            assert False

    def test_tree_download(self):
        assert self.reddit_secret is not None
        conversations = download_conversations("Trump", platform=PLATFORM.REDDIT)
        assert len(conversations) > 0

    def test_post_message(self):
        pass

    def test_download_daily(self):
        assert self.reddit_secret is not None, "environment variable not set"
        conversations = download_daily_sample_conversations(platform=PLATFORM.REDDIT,
                                                            language=LANGUAGE.ENGLISH,
                                                            min_results=5)
        assert len(conversations) > 0

    def test_get_user_conversations(self):
        assert self.reddit_secret is not None, "environment variable not set"
        conversations = get_user_conversations("CalmAsTheSea")
        assert len(conversations) > 0


if __name__ == '__main__':
    unittest.main()
