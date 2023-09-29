import logging

from api_settings import MT_STUDY_DAILY_FLOWS_NEEDED
from datasource.mastodon.download_conversations_mastodon import download_conversations_mstd
from datasource.mastodon.download_daily_political_sample_mstd import MTSampler
from datasource.reddit.download_conversations_reddit import search_r_all, download_conversations_by_id
from datasource.reddit.download_daily_political_rd_sample import RD_Sampler
from datasource.reddit.download_timelines_reddit import download_timelines_reddit
from datasource.twitter.download_conversations_twitter import download_conversations_tw
from datasource.twitter.download_daily_political_sample import download_daily_political_sample
from datasource.twitter.download_timelines_twitter import update_timelines_twitter
from delab_trees.delab_tree import DelabTree
from models.language import LANGUAGE
from models.platform import PLATFORM

logger = logging.getLogger(__name__)


def download_conversations(topic_string, query_string, request_id=-1, language=LANGUAGE.ENGLISH, max_data=False,
                           fast_mode=False, conversation_filter=None, tweet_filter=None, platform=PLATFORM.TWITTER,
                           recent=True):
    """
    This is a proxy to download conversations from twitter, reddit respectively with the same interface
    :param topic_string:
    :param query_string:
    :param request_id:
    :param language:
    :param max_data:
    :param fast_mode:
    :param conversation_filter:
    :param tweet_filter:
    :param platform:
    :param recent:
    :return:
    """
    """
    if fast_mode:
        max_number_of_candidates = 100
        min_conversation_length = 3
        max_conversation_length = 100
        if platform == PLATFORM.TWITTER:
            download_conversations_tw(topic_string, query_string, request_id, language, max_data,
                                      fast_mode, conversation_filter, tweet_filter, platform,
                                      recent, min_conversation_length=min_conversation_length,
                                      max_number_of_candidates=max_number_of_candidates)
        else:
            search_r_all(query_string, request_id, topic_string, min_conversation_length=min_conversation_length,
                         max_conversation_length=max_conversation_length,
                         max_number_of_candidates=max_number_of_candidates, tweet_filter=tweet_filter, recent=recent)
    """

    if platform == PLATFORM.TWITTER:
        download_conversations_tw(topic_string=topic_string, query_string=query_string, request_id=request_id,
                                  language=language, max_data=max_data,
                                  conversation_filter=conversation_filter,
                                  tweet_filter=tweet_filter, platform=platform,
                                  recent=recent)
    elif platform == PLATFORM.REDDIT:
        search_r_all(query_string, request_id, topic_string, tweet_filter=tweet_filter)
    elif platform == PLATFORM.MASTODON:
        download_conversations_mstd(query=query_string, topic=topic_string)


def download_timelines(simple_request_id, platform: PLATFORM):
    if platform == PLATFORM.TWITTER:
        update_timelines_twitter(simple_request_id)
    if platform == PLATFORM.REDDIT:
        download_timelines_reddit(simple_request_id)


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


def update_conversations(conversation_ids: list[int], platform: PLATFORM):
    if platform == PLATFORM.REDDIT:
        download_conversations_by_id(conversation_ids)
    else:
        raise NotImplementedError()
