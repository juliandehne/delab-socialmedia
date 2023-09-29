import logging
import re
import time

from requests import HTTPError

from delab.corpus.DelabTreeDAO import set_up_topic_and_simple_request
from delab.corpus.download_exceptions import ConversationNotInRangeException
from delab.corpus.filter_conversation_trees import solve_orphans
from delab.delab_enums import PLATFORM, LANGUAGE, TWEET_RELATIONSHIPS
from delab.models import TwTopic, SimpleRequest
from delab.tw_connection_util import DelabTwarc
from delab_trees.recursive_tree.recursive_tree import TreeNode
from django_project.settings import MAX_CANDIDATES, MAX_CONVERSATION_LENGTH, MIN_CONVERSATION_LENGTH
from util.abusing_lists import powerset

logger = logging.getLogger(__name__)


def download_conversations_tw(topic_string, query_string, request_id=-1, language=LANGUAGE.ENGLISH, max_data=False,
                              conversation_filter=None, tweet_filter=None, platform=PLATFORM.TWITTER,
                              recent=True,
                              max_conversation_length=MAX_CONVERSATION_LENGTH,
                              min_conversation_length=MIN_CONVERSATION_LENGTH,
                              max_number_of_candidates=MAX_CANDIDATES, persist=True):
    """
     @param persist: store the downloaded trees in db
     @param recent: use the recent api from twitter which is faster and more current
     @param conversation_filter: this takes on a partial function that rejects a TWConversationTree based on some criteria
                                 before saving it
     @param tweet_filter: this takes on a partial function that takes on a tweet, storing it and doing some additional task
                         before returning the now persistent tweet
     @param platform: reddit or twitter
     @param max_data: if it is set to true, the powerset (all combinations) of the query words is computed
     @param language: en, de or others
     @param request_id: if > 0 this is the reference to the SimpleRequest table that is filled when using the website
     @param query_string: the query used to find tweets in twitter
     @param topic_string: the title of the the topic as string
     @param max_number_of_candidates: the number of tweets used as candidates for a conversation
     @param min_conversation_length: this restricts conversations with too few posts,
            it should be noted that this is no flow analysis
     @param max_conversation_length: this restricts conversations with too many posts
     """
    if query_string is None or query_string.strip() == "":
        return False

    simple_request, topic = set_up_topic_and_simple_request(query_string, request_id, topic_string)

    twarc = DelabTwarc()

    # download the conversations
    if max_data:
        # this computes the powerset of the queried words
        pattern = r'[\(\)\[\]]'
        bag_of_words = re.sub(pattern, '', query_string).split(" ")
        combinations = list(powerset(bag_of_words))
        combinations_l = len(combinations) - 1
        combination_counter = 0
        for hashtag_set in combinations:
            if len(hashtag_set) > 0:
                combination_counter += 1
                new_query = " ".join(hashtag_set)
                filter_conversations(twarc, new_query, topic, simple_request, platform, language=language,
                                     conversation_filter=conversation_filter,
                                     tweet_filter=tweet_filter, recent=recent,
                                     max_conversation_length=max_conversation_length,
                                     min_conversation_length=min_conversation_length,
                                     max_number_of_candidates=max_number_of_candidates, persist=persist)
                logger.debug("FINISHED combination {}/{}".format(combination_counter, combinations_l))
    else:
        # in case max_data is false we don't compute the powerset of the hashtags
        filter_conversations(twarc, query_string, topic, simple_request, platform, language=language,
                             conversation_filter=conversation_filter,
                             tweet_filter=tweet_filter, recent=recent, max_conversation_length=max_conversation_length,
                             min_conversation_length=min_conversation_length,
                             max_number_of_candidates=max_number_of_candidates, persist=persist)


def filter_conversations(twarc,
                         query,
                         topic,
                         simple_request,
                         platform,
                         max_conversation_length=MAX_CONVERSATION_LENGTH,
                         min_conversation_length=MIN_CONVERSATION_LENGTH,
                         language=LANGUAGE.ENGLISH,
                         max_number_of_candidates=MAX_CANDIDATES,
                         conversation_filter=None,
                         tweet_filter=None, recent=True, persist=True):
    """
    @param persist:
    @see download_conversations_tw
    @param twarc:
    @param query:
    @param topic:
    @param simple_request:
    @param platform:
    @param max_conversation_length:
    @param min_conversation_length:
    @param language:
    @param max_number_of_candidates:
    @param conversation_filter:
    @param tweet_filter:
    @param recent:
    @return:
    """

    # download the tweets that fulfill the query as candidates for whole conversation trees
    candidates, n_pages = download_conversation_representative_tweets(twarc, query, max_number_of_candidates, language,
                                                                      recent=recent)
    downloaded_tweets = 0
    n_dismissed_candidates = 0

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

                # apply the conversation filter
                if conversation_filter is not None:
                    root_node = conversation_filter(root_node)

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
                        save_tree_to_db(root_node, topic, simple_request, conversation_id, platform,
                                        candidate_id=int(candidate["id"]),
                                        tweet_filter=tweet_filter)
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


def save_tree_to_db(root_node: TreeNode,
                    topic: TwTopic,
                    simple_request: SimpleRequest,
                    conversation_id: int,
                    platform: PLATFORM, candidate_id=None,
                    tweet_filter=None):
    """ This method persist a conversation tree in the database
        Parameters
        ----------
        :param root_node : TwConversationTree
        :param topic : the topic of the query
        :param simple_request: the query string in order to link the view
        :param conversation_id: the conversation id of the candidate tweet that was found with the request
        :param platform: this was added to allow for a "fake" delab platform to come in
        :param tweet_filter: a function that takes a tweet model object and validates it (returns None if not)
        :param candidate_id
    """
    # TODO run some tree validations
    store_tree_data(conversation_id, platform, root_node, simple_request, topic, candidate_id, tweet_filter)
