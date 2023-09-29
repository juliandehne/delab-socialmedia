import logging
from datetime import datetime, timedelta
from random import choice

from delab.corpus.download_exceptions import ConversationNotInRangeException
from delab.corpus.sample_political_keywords import topics, search_phrases
from delab.corpus.twitter.download_conversations_twitter import download_conversation_representative_tweets, \
    download_conversation_as_tree
from delab.delab_enums import LANGUAGE
from delab.tw_connection_util import DelabTwarc
from delab_trees.delab_tree import DelabTree

logger = logging.getLogger(__name__)


def download_daily_political_sample(language, topic_string) -> list[DelabTree]:
    if language != LANGUAGE.ENGLISH:
        raise NotImplementedError()
    random_topic = choice(topics)
    phrases = search_phrases[random_topic]
    random_phrase = choice(phrases)
    # query = construct_daily_query(random_phrase)
    query = random_phrase
    downloaded_trees = download_twitter_sample(query=query)
    result = list(map(lambda x: DelabTree.from_recursive_tree(x), downloaded_trees))
    return result


def construct_daily_query(search_query):
    # Get the current date
    current_date = datetime.now().date()

    # Specify the date range
    since_date = current_date.strftime("%Y-%m-%d")
    until_date = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")

    # Construct the search query with the date range
    search_query_with_dates = f'{search_query} since:{since_date} until:{until_date}'

    return search_query_with_dates


def download_twitter_sample(query):
    twarc = DelabTwarc()
    # download the tweets that fulfill the query as candidates for whole conversation trees
    candidates, n_pages = download_conversation_representative_tweets(twarc, query, n_candidates=100)
    downloaded_tweets = 0
    n_dismissed_candidates = 0

    downloaded_trees = []
    # iterate through the candidates
    for candidate in candidates:
        if len(downloaded_trees) > 5:
            break
        try:
            reply_count = candidate["public_metrics"]["reply_count"]
            # apply the length constraints early
            if reply_count > 5:
                conversation_id = candidate["conversation_id"]

                # download the other tweets from the conversation as a TWConversationTree
                root_node = download_conversation_as_tree(twarc, conversation_id, max_replies=100)

                # skip the processing if there was a problem with constructing the conversation tree
                if root_node is None:
                    logger.error("found conversation_id that could not be processed")
                    continue
                else:
                    # some communication code in order to see what kinds of trees are being downloaded
                    flat_tree_size = root_node.flat_size()
                    logger.debug("found tree with size: {}".format(flat_tree_size))
                    depth = root_node.compute_max_path_length()
                    logger.debug("found tree with depth: {}".format(depth))
                    # maybe add this check to a later point but this is easy for now
                    if depth > 5:
                        downloaded_tweets += flat_tree_size
                        downloaded_trees.append(root_node)
                    else:
                        n_dismissed_candidates += 1
            else:
                n_dismissed_candidates += 1
        except ConversationNotInRangeException as ex:
            n_dismissed_candidates += 1

    logger.debug("{} of {} candidates were dismissed".format(n_dismissed_candidates, len(candidates)))
    return downloaded_trees
