import logging

import mastodon
from mastodon import MastodonServiceUnavailableError

from daily_sampler import download_samples, check_general_tree_requirements
from datasource.mastodon.download_conversations_mastodon import download_conversations_mstd
from datasource.mastodon.download_daily_political_sample_mstd import MTSampler
from datasource.reddit.download_conversations_reddit import search_r_all
from datasource.reddit.download_daily_political_rd_sample import RD_Sampler
from datasource.twitter.download_conversations_twitter import download_conversations_tw
from download_exceptions import NoDailySubredditAvailableException, NoDailyMTHashtagsAvailableException
from models.language import LANGUAGE
from models.platform import PLATFORM

logger = logging.getLogger(__name__)


def download_conversations(query_string="Politik",
                           platform=PLATFORM.REDDIT,
                           language=LANGUAGE.ENGLISH,
                           recent=True,
                           max_conversations=30,
                           connector=None
                           ):
    """
    This is a proxy to download conversations from twitter, reddit respectively with the same interface
    :param connector: connector object, twarc for Twitter, praw for reddit and mastodon for Mastodon, see README for examples
    :param query_string: the query string to be searched
    :param language: reddit needs the language as default if language is not set in reddit response
    :param platform: twitter, mastodon or reddit currently
    :param recent: use recent version of apis, also prioritize recent events
    :param max_conversations: max number of conversations. Cuts of querying before checking tree requirements!
    :return:
    """
    result = []

    if platform == PLATFORM.TWITTER:
        result = download_conversations_tw(connector,
                                           query_string=query_string,
                                           language=language,
                                           platform=platform,
                                           recent=recent)
    elif platform == PLATFORM.REDDIT:
        result = search_r_all(query_string,
                              max_conversations=max_conversations,
                              recent=recent,
                              language=language,
                              reddit=connector)
    elif platform == PLATFORM.MASTODON:
        result = download_conversations_mstd(query=query_string, max_conversations=max_conversations,
                                             mastodon=connector)

    result_filtered = [x for x in result if x.total_number_of_posts() > 2 and x.validate(verbose=False)]

    return result_filtered


def download_daily_sample_conversations(platform, min_results, language, connector=None):
    # reset the list of subreddits to download
    if platform == PLATFORM.REDDIT:
        RD_Sampler.daily_en_subreddits = {}
        RD_Sampler.daily_de_subreddits = {}
    elif platform == PLATFORM.MASTODON:
        MTSampler.daily_en_hashtags = {}
        MTSampler.daily_de_hashtags = {}
    # Perform 100 runs of the function and measure the time taken
    try:
        # download_mturk_sample_helper = partial(download_mturk_samples, platform, min_results, language, persist)
        # execution_time = timeit.timeit(download_mturk_sample_helper, number=n_runs)
        results = download_samples(platform, min_results, language, connector)
        return results
        # average_time = (execution_time / 100) / 60
        # print("Execution time:", execution_time, "seconds")
        # print("Average Execution time:", average_time, "minutes")
    except NoDailySubredditAvailableException as no_more_subreddits_to_try:
        logger.debug("Tried all subreddits for language {}".format(no_more_subreddits_to_try.language))
    except NoDailyMTHashtagsAvailableException as no_more_hashtags_to_try:
        logger.debug("Tried all hashtags for language {}".format(no_more_hashtags_to_try.language))
    except TimeoutError:
        logger.debug("Downloading timeline took too long. Skipping hashtag {}")
        return []
    except MastodonServiceUnavailableError as mastodonerror:
        logger.error("Mastodon seemed not to be available {}".format(mastodonerror))
    except mastodon.errors.MastodonAPIError as mastodonerror:
        logger.error("Mastodon seemed not to be available {}".format(mastodonerror))
    except mastodon.errors.MastodonNetworkError as mastodonerror:
        logger.error("Mastodon seemed not to be available {}".format(mastodonerror))
