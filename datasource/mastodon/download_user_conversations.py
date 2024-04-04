from connection_util import create_mastodon
from datasource.mastodon.download_conversations_mastodon import find_context, toots_to_tree


def download_user_conversations(username, mastodon=None, since="2023-01-01", max_conversations=1000):
    """
    Usage
    trees = download_user_conversations(username="the_user", mastodon=mastodon_instance, since="2023-01-01", max_conversations=5)

    :param username:
    :param mastodon:
    :param since:
    :param max_conversations:
    :return:
    """
    if mastodon is None:
        mastodon = create_mastodon()

    user = mastodon.account_search(username, limit=1)
    if not user:
        return []  # Return an empty list if user is not found

    user_id = user[0]['id']
    statuses = mastodon.account_statuses(user_id, since_id=since)
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
