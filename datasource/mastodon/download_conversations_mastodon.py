import logging
from delab.corpus.DelabTreeDAO import persist_tree, set_up_topic_and_simple_request, check_general_tree_requirements
from delab_trees.delab_tree import DelabTree
from delab.tw_connection_util import create_mastodon
from bs4 import BeautifulSoup
from delab.delab_enums import PLATFORM, LANGUAGE
import pandas as pd
import signal
from datasource.mastodon import MastodonNetworkError

logger = logging.getLogger(__name__)


def download_conversations_mstd(query, topic, since=None):
    mastodon = create_mastodon()
    download_conversations_to_search(query=query, mastodon=mastodon, topic=topic, since=since)


def download_conversations_to_search(query, mastodon, topic, since, daily_sample=False):
    statuses = download_timeline(query=query, mastodon=mastodon, since=since)
    contexts = []
    trees = []

    for status in statuses:
        if status in contexts:
            continue
        else:
            context = find_context(status, mastodon)
            if context is None:
                continue
            contexts.append(context)

    for context in contexts:
        conversation_id = context['root']["id"]
        tree = toots_to_tree(context=context, conversation_id=conversation_id)
        if tree is not None:
            trees.append(tree)
            if not daily_sample:
                save_tree_to_db(tree=tree, topic=topic, query=query)
    return trees


def timeout_handler(signum, frame):
    raise TimeoutError("process took too long")


def download_timeline(query, mastodon, since):
    timeout_seconds = 30
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout_seconds)
    try:
        timeline = mastodon.timeline_hashtag(hashtag=query, limit=40, since_id=since)
    except TimeoutError:
        logger.debug("Downloading timeline took too long. Skipping hashtag {}".format(query))
        return []
    return timeline


def find_context(status, mastodon):
    timeout_seconds = 20
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout_seconds)
    context = {'root': status}
    try:
        if status['in_reply_to_id'] is None:
            if status['replies_count'] == 0:
                return None
            context.update(mastodon.status_context(status["id"]))
        else:
            original_context = {'origin': status}
            original_context.update(mastodon.status_context(status["id"]))
            root = get_root(original_context)
            if root is None:
                return None
            context['root'] = root
            context.update(mastodon.status_context(root["id"]))

    except TimeoutError:
        logger.debug("Downloading context took too long. Skipping status {}".format(status['url']))
        return None
    except MastodonNetworkError as neterr:
        logger.debug("API threw following error: {}".format(neterr))
        return None

    return context


def get_conversation_id(context):
    # mastodon api has no option to get conversation_id by status --> using id of root toot as conversation_id
    ancestors = context["ancestors"]
    origin = context["root"]
    if origin["in_reply_to_id"] is None:
        return context["origin"]["id"]
    else:
        for status in ancestors:
            if status["in_reply_to_id"] is None:
                return status["id"]


def get_root(context):
    ancestors = context["ancestors"]
    origin = context["origin"]
    if origin["in_reply_to_id"] is not None and len(context['ancestors']) == 0:
        print("Conversation has no root!")
        return None
    for toot in ancestors:
        if toot["in_reply_to_id"] is None:
            return toot
    print("Couldn't find root!")
    return None


def toots_to_tree(context, conversation_id):
    root = context["root"]
    descendants = context["descendants"]
    ancestors = context["ancestors"]  # should be emtpy
    tree_context = []
    text = content_to_text(root["content"])
    lang = root['language']
    if lang is None:
        lang = LANGUAGE.UNKNOWN
    tree_status = {'tree_id': conversation_id,
                   'post_id': str(root['id']),
                   'parent_id': str(root['in_reply_to_id']),
                   'text': text,
                   'created_at': root['created_at'],
                   'author_id': root['account']['id'],
                   'lang': lang,
                   'url': root["url"]}
    tree_context.append(tree_status)
    for ancestor in ancestors:
        lang = ancestor['lang']
        if lang is None:
            lang = LANGUAGE.UNKNOWN
        text = content_to_text(ancestor["content"])
        tree_status = {'tree_id': conversation_id,
                       'post_id': str(ancestor['id']),
                       'parent_id': str(ancestor['in_reply_to_id']),
                       'text': text,
                       'created_at': ancestor['created_at'],
                       'author_id': ancestor['account']['id'],
                       'lang': lang,
                       'url': ancestor["url"]}
        tree_context.append(tree_status)
    for descendant in descendants:
        lang = descendant['language']
        if lang is None:
            lang = LANGUAGE.UNKNOWN
        text = content_to_text(descendant["content"])
        tree_status = {'tree_id': conversation_id,
                       'post_id': str(descendant['id']),
                       'parent_id': str(descendant['in_reply_to_id']),
                       'text': text,
                       'created_at': descendant['created_at'],
                       'author_id': descendant['account']['id'],
                       'lang': lang,
                       'url': descendant["url"]}
        tree_context.append(tree_status)

    context_df = pd.DataFrame(tree_context)
    # context_df_clean = pre_process_df(context_df)
    tree = DelabTree(context_df)
    if not check_general_tree_requirements(delab_tree=tree, platform=PLATFORM.MASTODON):
        return None
    return tree


def pre_process_df(context_df):
    """
    convert float and int ids to str
    :return:
    """
    if context_df["parent_id"].dtype != "object":
        df_parent_view = context_df.loc[:, "parent_id"]
        context_df.loc[:, "parent_id"] = df_parent_view.astype(float).astype(str)
    if context_df["post_id"].dtype != "object":
        df_post_view = context_df.loc[:, "post_id"]
        context_df.loc[:, "post_id"] = df_post_view.astype(float).astype(str)
    else:
        assert context_df["parent_id"].dtype == "object" and context_df[
            "post_id"].dtype == "object", "post_id and parent_id need to be both float or str"
    return context_df


def save_tree_to_db(tree, topic, query):
    simple_request, tw_topic = set_up_topic_and_simple_request(query_string=query, request_id=-1, topic_string=topic)
    persist_tree(tree=tree, platform=PLATFORM.MASTODON, simple_request=simple_request, topic=tw_topic)


def content_to_text(content):
    # content is html string --> get only necessary text
    soup = BeautifulSoup(content, 'html.parser')
    text = soup.get_text()
    return text
