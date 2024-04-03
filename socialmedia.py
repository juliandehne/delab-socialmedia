import logging

from api_settings import MT_STUDY_DAILY_FLOWS_NEEDED
from datasource.mastodon.download_conversations_mastodon import download_conversations_mstd
from datasource.mastodon.download_daily_political_sample_mstd import MTSampler
from datasource.reddit.download_conversations_reddit import search_r_all
from datasource.reddit.download_daily_political_rd_sample import RD_Sampler
from datasource.twitter.download_conversations_twitter import download_conversations_tw
from datasource.twitter.download_daily_political_sample import download_daily_political_sample
from delab_trees.delab_tree import DelabTree
from models.language import LANGUAGE
from models.platform import PLATFORM

logger = logging.getLogger(__name__)


def download_conversations(connector, query_string="Politik",
                           language=LANGUAGE.ENGLISH,
                           platform=PLATFORM.TWITTER,
                           recent=True, max_conversations=5):
    """
    This is a proxy to download conversations from twitter, reddit respectively with the same interface
    :param connector: connector object, twarc for Twitter, praw for reddit and mastodon for Mastodon, see README for examples
    :param query_string:
    :param language:
    :param platform: twitter, mastodon or reddit currently
    :param recent: use recent version of apis, also prioritize recent events
    :param max_conversations: max number of conversations. Cuts of querying before checking tree requirements!
    :return:
    """

    if platform == PLATFORM.TWITTER:
        download_conversations_tw(connector, query_string=query_string,
                                  language=language, platform=platform,
                                  recent=recent)
    elif platform == PLATFORM.REDDIT:
        search_r_all(connector, query_string, recent=recent, language=language)
    elif platform == PLATFORM.MASTODON:
        download_conversations_mstd(connector, query=query_string, max_conversations=max_conversations)


def download_daily_sample(topic_string,
                          platform: PLATFORM,
                          language=LANGUAGE.ENGLISH,
                          max_results=MT_STUDY_DAILY_FLOWS_NEEDED) -> list[DelabTree]:
    """

    @param topic_string:
    @param platform:
    @param language:
    @param max_results: the maximum number of suitable trees to be found for a given platform and a day
    @return:
    """
    if platform == platform.TWITTER:
        return download_daily_political_sample(language, topic_string=topic_string)
    if platform == platform.REDDIT:
        sampler = RD_Sampler(language)
        return sampler.download_daily_rd_sample(max_results=max_results)
    if platform == platform.MASTODON:
        sampler = MTSampler(language=language)
        return sampler.download_daily_political_sample_mstd(topic_string)
    else:
        raise NotImplementedError()
