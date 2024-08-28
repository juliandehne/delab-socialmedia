from connection_util import get_praw
from datasource.reddit.download_conversations_reddit import compute_reddit_tree
from delab_trees.delab_tree import DelabTree


def get_conversations_by_url(url, reddit=None):
    if reddit is None:
        reddit = get_praw()

    original_comment = reddit.submission(url=url)
    recursive_tree = compute_reddit_tree(original_comment)
    delab_tree = DelabTree.from_recursive_tree(recursive_tree)
    return delab_tree
