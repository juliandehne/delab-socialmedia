import logging
import time

from requests import HTTPError

from api_settings import MAX_CONVERSATION_LENGTH, MIN_CONVERSATION_LENGTH, MAX_CANDIDATES
from connection_util import DelabTwarc
from delab_trees.recursive_tree.recursive_tree import TreeNode
from delab_trees.recursive_tree.recursive_tree_util import solve_orphans
from download_exceptions import ConversationNotInRangeException
from models.language import LANGUAGE
from models.platform import PLATFORM

logger = logging.getLogger(__name__)


def download_conversations_tw(twarc, query_string, language=LANGUAGE.ENGLISH,
                              platform=PLATFORM.TWITTER,
                              recent=True,
                              min_conversation_length=MIN_CONVERSATION_LENGTH,
                              max_number_of_candidates=MAX_CANDIDATES):
    """
     @param recent: use the recent api from twitter which is faster and more current
     @param platform: reddit or twitter
     @param language: en, de or others
     @param query_string: the query used to find tweets in twitter
     @param max_number_of_candidates: the number of tweets used as candidates for a conversation
     @param min_conversation_length: this restricts conversations with too few posts,
            it should be noted that this is no flow analysis
     """
    if twarc is None:
        twarc = DelabTwarc()

    if query_string is None or query_string.strip() == "":
        return False

    # in case max_data is false we don't compute the powerset of the hashtags
    filter_conversations(twarc, query_string, platform, language=language, recent=recent,
                         min_conversation_length=min_conversation_length,
                         max_number_of_candidates=max_number_of_candidates)


def filter_conversations(twarc,
                         query,
                         max_conversation_length=MAX_CONVERSATION_LENGTH,
                         min_conversation_length=MIN_CONVERSATION_LENGTH,
                         language=LANGUAGE.ENGLISH,
                         max_number_of_candidates=MAX_CANDIDATES, recent=True):
    """
    @see download_conversations_tw
    @param twarc:
    @param query:
    @param max_conversation_length:
    @param min_conversation_length:
    @param language:
    @param max_number_of_candidates:
    @param recent:
    @return:
    """

    # download the tweets that fulfill the query as candidates for whole conversation trees
    candidates, n_pages = download_conversation_representative_tweets(twarc, query, max_number_of_candidates,
                                                                      language,
                                                                      recent=recent)
    downloaded_tweets = 0
    n_dismissed_candidates = 0

    result = []
    # iterate through the candidates
    for candidate in candidates:
        try:
            reply_count = candidate["public_metrics"]["reply_count"]
            # apply the length constraints early
            if (min_conversation_length / 2) < reply_count < max_conversation_length:
                logger.debug("selected candidate tweet {}".format(candidate))
                conversation_id = candidate["conversation_id"]

                # download the other tweets from the conversation as a TWConversationTree
                root_node = download_conversation_as_tree(twarc, conversation_id, max_conversation_length)

                # skip the processing if there was a problem with constructing the conversation tree
                if root_node is None:
                    logger.error("found conversation_id that could not be processed")
                    continue
                else:
                    # some communication code in order to see what kinds of trees are being downloaded
                    flat_tree_size = root_node.flat_size()
                    logger.debug("found tree with size: {}".format(flat_tree_size))
                    logger.debug("found tree with depth: {}".format(root_node.compute_max_path_length()))
                    downloaded_tweets += flat_tree_size
                    if min_conversation_length < flat_tree_size < max_conversation_length:
                        result.append(root_node)
                        logger.debug("found suitable conversation and saved to db {}".format(conversation_id))
                        # for debugging you can ascii art print the downloaded conversation_tree
                        # root_node.print_tree(0)
            else:
                n_dismissed_candidates += 1
        except ConversationNotInRangeException as ex:
            n_dismissed_candidates += 1
            logger.debug("conversation was dismissed because it was longer than {}".format(max_conversation_length))
    logger.debug("{} of {} candidates were dismissed".format(n_dismissed_candidates, len(candidates)))


def ensuring_tweet_lookup_quota(n_pages, recent, tweet_lookup_request_counter):
    if tweet_lookup_request_counter - n_pages <= 0:
        tweet_lookup_request_counter = 250
        if recent:
            tweet_lookup_request_counter = 400
        logger.error("going to sleep between processing candidates because of rate limitations")
        time.sleep(300 * 60)
    return tweet_lookup_request_counter


def download_conversation_representative_tweets(twarc, query, n_candidates,
                                                language=LANGUAGE.ENGLISH, recent=True):
    """
    :param recent:
    :param twarc:
    :param query:
    :param n_candidates:
    :param language:
    :return:
    """
    min_page_size = 10
    max_page_size = 500
    if n_candidates > max_page_size:
        page_size = 500
    else:
        page_size = n_candidates
    assert page_size >= min_page_size

    twitter_accounts_query = query + " lang:" + language
    logger.debug(twitter_accounts_query)
    candidates = []
    try:
        if recent:
            page_size = 100
            candidates = twarc.search_recent(query=twitter_accounts_query,
                                             tweet_fields="conversation_id,author_id,public_metrics")
        else:
            candidates = twarc.search_all(query=twitter_accounts_query,
                                          tweet_fields="conversation_id,author_id,public_metrics",
                                          max_results=page_size)
    except HTTPError as httperror:
        print(httperror)
    result = []
    n_pages = 1
    for candidate in candidates:
        result += candidate.get("data", [])
        n_pages += 1
        # logger.debug("number of candidates downloaded: {}".format(str(count)))
        if n_pages * page_size > n_candidates:
            break

    return result, n_pages


def download_conversation_as_tree(twarc, conversation_id, max_replies, root_data=None):
    """
    this downloads a candidate tweet from the conversation and uses its conversation id for the conversation_download
    :param twarc:
    :param conversation_id:
    :param max_replies:
    :param root_data:
    :return:
    """
    if root_data is None:
        results = next(twarc.tweet_lookup(tweet_ids=[conversation_id]))
        if "data" in results:
            root_data = results["data"][0]
        else:
            return None
    return create_tree_from_raw_tweet_stream(conversation_id, max_replies, root_data, twarc)


def create_tree_from_raw_tweet_stream(conversation_id, max_replies, root_data, twarc):
    """
    this uses the conversation_id to download the whole conversation from twitter as far as available
    :param conversation_id:
    :param max_replies:
    :param root_data:
    :param twarc:
    :return:
    """
    tweets = []
    for result in twarc.search_all("conversation_id:{}".format(conversation_id)):
        tweets = tweets + result.get("data", [])
        check_conversation_max_size(max_replies, tweets)
    root, orphans = create_conversation_tree_from_tweet_data(conversation_id, root_data, tweets)
    return root


def create_conversation_tree_from_tweet_data(conversation_id, root_tweet, tweets):
    """
    this function constructs a TwConversationTree structure out of the unsorted list of tweets
    @param conversation_id:
    @param root_tweet:
    @param tweets:
    @return: (TwConversationTree, [orphan_data])
    """
    # sort tweets by creation date in order to speed up the tree construction
    tweets.sort(key=lambda x: x["created_at"], reverse=False)
    root = TreeNode(root_tweet, root_tweet["id"])
    orphans = []
    for item in tweets:
        # node_id = item["author_id"]
        # parent_id = item["in_reply_to_user_id"]
        node_id = int(item["id"])
        parent_id, parent_type = get_priority_parent_from_references(item["referenced_tweets"])
        # parent_id = item["referenced_tweets.id"]
        node = TreeNode(item, node_id, parent_id, parent_type=parent_type)
        # IF NODE CANNOT BE PLACED IN TREE, ORPHAN IT UNTIL ITS PARENT IS FOUND
        if not root.find_parent_of(node):
            orphans.append(node)
    logger.info('{} orphaned tweets for conversation {} before resolution'.format(len(orphans), conversation_id))
    orphan_added = True
    while orphan_added:
        orphan_added, orphans = solve_orphans(orphans, root)
    if len(orphans) > 0:
        logger.error('{} orphaned tweets for conversation {}'.format(len(orphans), conversation_id))
        logger.error('{} downloaded tweets'.format(len(tweets)))
    return root, orphans


def check_conversation_max_size(max_replies, tweets):
    conversation_size = len(tweets)
    if conversation_size >= max_replies > 0:
        raise ConversationNotInRangeException(conversation_size)


def get_priority_parent_from_references(references):
    """
    This constructs the parent relationship between the tweets in the tree.
    It is primarily based on the reply to relationship but if this does not exist,
    the retweet or quote rel is used
    @param references:
    @return:
    """
    reference_types = [ref["type"] for ref in references]
    replied_tos = [int(ref["id"]) for ref in references if ref["type"] == TWEET_RELATIONSHIPS.REPLIED_TO]
    retweeted_tos = [int(ref["id"]) for ref in references if ref["type"] == TWEET_RELATIONSHIPS.RETWEETED]
    quoted_tos = [int(ref["id"]) for ref in references if ref["type"] == TWEET_RELATIONSHIPS.QUOTED]
    if TWEET_RELATIONSHIPS.REPLIED_TO in reference_types:
        return replied_tos[0], TWEET_RELATIONSHIPS.REPLIED_TO
    if TWEET_RELATIONSHIPS.QUOTED in reference_types:
        return quoted_tos[0], TWEET_RELATIONSHIPS.QUOTED
    if TWEET_RELATIONSHIPS.RETWEETED in reference_types:
        return retweeted_tos[0], TWEET_RELATIONSHIPS.RETWEETED
    raise Exception("no parent found")


class TWEET_RELATIONSHIPS:
    REPLIED_TO = "replied_to",
    QUOTED = "quoted",
    RETWEETED = "retweeted"
