import logging
import os

import praw
import yaml
from mastodon import Mastodon
from twarc import Twarc2

logger = logging.getLogger(__name__)


class ConnectionUtil:

    @staticmethod
    def get_secret(yaml_path):
        consumer_key = os.environ.get("CONSUMER_KEY")
        consumer_secret = os.environ.get("CONSUMER_SECRET")
        with open(yaml_path) as f:
            my_dict = yaml.safe_load(f)
            if consumer_key != "":
                consumer_key = my_dict.get("consumer_key")
            if consumer_secret != "":
                consumer_secret = my_dict.get("consumer_secret")
            access_token = my_dict.get("access_token")
            access_token_secret = my_dict.get("access_token_secret")
            bearer_token = my_dict.get("bearer_token")
        return access_token, access_token_secret, bearer_token, consumer_key, consumer_secret

    @staticmethod
    def get_reddit_secret(yaml_path):
        reddit_secret = os.environ.get("reddit_secret")
        reddit_script_id = os.environ.get("reddit_script_id")
        reddit_user = os.environ.get("reddit_user_name")
        reddit_password = os.environ.get("reddit_password")

        with open(yaml_path) as f:
            my_dict = yaml.safe_load(f)
            if reddit_secret != "":
                reddit_secret = my_dict.get("reddit_secret")
            if reddit_script_id != "":
                reddit_script_id = my_dict.get("reddit_script_id")
            if reddit_user != "":
                reddit_user = my_dict.get("reddit_user_name")
            if reddit_password != "":
                reddit_password = my_dict.get("reddit_password")
        return reddit_secret, reddit_script_id, reddit_user, reddit_password

    @staticmethod
    def get_mastodon_secret():
        client_id = os.environ.get("mst_client_id")
        client_secret = os.environ.get("mst_client_secret")
        access_token = os.environ.get("mst_access_token")
        return access_token, client_id, client_secret


class DelabTwarc(Twarc2):
    def __init__(self, access_token=None, access_token_secret=None, bearer_token=None, consumer_key=None,
                 consumer_secret=None, use_yaml=False, yaml_path=None):
        """
        create the Twitter connector
        :param access_token:
        :param access_token_secret:
        :param bearer_token:
        :param consumer_key:
        :param consumer_secret:
        :param use_yaml:
        :param yaml_path:
        """
        if use_yaml:
            access_token, access_token_secret, bearer_token, consumer_key, consumer_secret = ConnectionUtil.get_secret(
                yaml_path)
        super().__init__(consumer_key, consumer_secret, access_token, access_token_secret, bearer_token)


def get_praw(reddit_secret=None, reddit_script_id=None, reddit_user=None, reddit_password=None, user_agent=None,
             use_yaml=False, yaml_path=None):
    """
    create the Reddit connector
    :param reddit_secret:
    :param reddit_script_id:
    :param reddit_user:
    :param reddit_password:
    :param user_agent:
    :param use_yaml:
    :param yaml_path:
    :return:
    """
    if use_yaml:
        user_agent = "django_script:de.uni-goettingen.delab:v0.0.1 (by u/CalmAsTheSea)"
        reddit_secret, reddit_script_id, reddit_user, reddit_password = ConnectionUtil.get_reddit_secret(yaml_path)
    reddit = praw.Reddit(client_id=reddit_script_id,
                         client_secret=reddit_secret,
                         user_agent=user_agent,
                         username=reddit_user,
                         password=reddit_password)
    return reddit


def create_mastodon(client_id=None,
                    client_secret=None,
                    access_token=None,
                    api_base_url="https://mastodon.social/",
                    use_yaml=False,
                    yaml_path=None):
    """
    Create the Mastodon connector
    You have to register your application in the mastodon web app first,
    (home/preferences/Development/new application)
    then save the necessary information in the file that is called
    :param client_id:
    :param client_secret:
    :param access_token:
    :param api_base_url:
    :param use_yaml:
    :param yaml_path:
    :return:
    """
    if client_id is None:
        access_token, client_id, client_secret = ConnectionUtil.get_mastodon_secret()
        if use_yaml:
            with open(yaml_path, 'r') as f:
                access = yaml.safe_load(f)
                client_id = access["client_id"],
                client_secret = access["client_secret"],
                access_token = access["access_token"],

    mastodon = Mastodon(client_id=client_id, client_secret=client_secret, access_token=access_token,
                        api_base_url=api_base_url
                        )
    return mastodon
