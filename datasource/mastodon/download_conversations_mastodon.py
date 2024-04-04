import logging
import signal

import pandas as pd
from bs4 import BeautifulSoup
from mastodon import MastodonNetworkError

from connection_util import create_mastodon
from delab_trees.delab_tree import DelabTree
from models.language import LANGUAGE

logger = logging.getLogger(__name__)


def download_conversations_mstd(query, mastodon=None, since=None, max_conversations=5):
    if mastodon is None:
        mastodon = create_mastodon()

    return download_conversations_to_search(query=query,
                                            mastodon=mastodon,
                                            since=since,
                                            max_conversations=max_conversations)


def download_conversations_to_search(query, mastodon, since, max_conversations=5):
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
        if len(trees) >= max_conversations:
            break

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


def content_to_text(content):
    # content is html string --> get only necessary text
    soup = BeautifulSoup(content, 'html.parser')
    text = soup.get_text()
    return text
