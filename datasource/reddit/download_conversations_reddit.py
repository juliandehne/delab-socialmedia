import datetime
import logging
import time
import prawcore
import pytz

from api_settings import MAX_CANDIDATES_REDDIT
from connection_util import get_praw
from delab_trees import TreeNode
from delab_trees.delab_tree import DelabTree
from delab_trees.recursive_tree.recursive_tree_util import solve_orphans
from models.language import LANGUAGE
from util.abusing_strings import convert_to_hash

"""
get the moderators like this
https://praw.readthedocs.io/en/stable/code_overview/other/subredditmoderation.html

for message in reddit.subreddit("mod").mod.inbox(limit=5):
    print(f"From: {message.author}, Body: {message.body}")
    for reply in message.replies:
        print(f"From: {reply.author}, Body: {reply.body}")

"""

logger = logging.getLogger(__name__)


def search_r_all(query: str, max_conversations=5, reddit=None, recent=True,
                 language=LANGUAGE.ENGLISH):
    """
    searches for query in reddit all
    :param max_conversations:
    :param reddit:
    :param query:
    :param recent:
    :param language:
    :return:
    """
    trees = []
    if reddit is None:
        reddit = get_praw()
    try:
        if recent:
            for submission in reddit.subreddit("all").search(query=query, limit=MAX_CANDIDATES_REDDIT,
                                                             sort="new"):
                result_tree = compute_reddit_tree(submission, language)
                trees.append(result_tree)
                if len(trees) >= max_conversations:
                    break
        else:
            for submission in reddit.subreddit("all").search(query=query, limit=MAX_CANDIDATES_REDDIT):
                result_tree = compute_reddit_tree(submission, language)
                trees.append(result_tree)
                if len(trees) >= max_conversations:
                    break
    except prawcore.exceptions.Redirect:
        logger.error("reddit with this name does not exist")

    result = [DelabTree.from_recursive_tree(x) for x in trees]
    return result


def download_subreddit(reddit, sub_reddit_string, language=LANGUAGE.ENGLISH,
                       hot=False):
    logger.debug("saving subreddit {}".format(sub_reddit_string))

    if reddit is None:
        reddit = get_praw()
    trees = []
    try:
        if not hot:
            count = 0
            for submission in reddit.subreddit(sub_reddit_string).top(limit=None):
                try:
                    count += 1
                    logger.debug("saving subreddit {}, submission {}".format(sub_reddit_string, count))
                    trees.append(compute_reddit_tree(submission, language))
                except prawcore.exceptions.TooManyRequests:
                    time.sleep(600)
        else:
            for submission in reddit.subreddit(sub_reddit_string).hot(limit=10):
                trees.append(compute_reddit_tree(submission, language))
    except prawcore.exceptions.Redirect:
        logger.error("reddit with this name does not exist{}".format(sub_reddit_string))
    except prawcore.exceptions.Forbidden:
        logger.error("reddit with this name could not be accessed {}".format(sub_reddit_string))
    result = [DelabTree.from_recursive_tree(x) for x in trees]
    return result


def compute_reddit_tree(submission, language=None):
    comments = sort_comments_for_db(submission)

    # root node
    author_id, author_name = compute_author_id(submission)
    tree_id = convert_to_hash(submission.fullname)
    root_node_id = convert_to_hash(submission.fullname)
    if hasattr(submission, 'lang'):
        submission_lang = submission.lang
    else:
        submission_lang = language
    data = {
        "tree_id": tree_id,
        "post_id": root_node_id,
        "text": submission.title + "\n" + submission.selftext,
        "author_id": author_id,
        "created_at": convert_time_stamp_to_django(submission),
        "tw_author__name": author_name,
        "rd_data": submission,
        "lang": submission_lang,
        "url": "https://reddit.com" + submission.permalink,
        "reddit_id": submission.id}
    root = TreeNode(data, root_node_id, tree_id=tree_id)
    orphans = []

    for comment in comments:
        # node_id = comment.id
        node_id = convert_to_hash(comment.fullname)
        # parent_id = comment.parent_id.split("_")[1]
        parent_id = convert_to_hash(comment.parent_id)
        comment_author_id, comment_author_name = compute_author_id(comment)
        if hasattr(comment, 'lang'):
            comment_lang = comment.lang
        else:
            comment_lang = language
        comment_data = {
            "tree_id": tree_id,
            "post_id": node_id,
            "text": comment.body,
            "author_id": comment_author_id,
            "tw_author__name": comment_author_name,
            "created_at": convert_time_stamp_to_django(comment),
            "parent_id": parent_id,
            "rd_data": comment,
            "lang": comment_lang,
            "url": "https://reddit.com" + comment.permalink,
            "reddit_id": comment.id}
        node = TreeNode(comment_data, node_id, parent_id, tree_id=tree_id)
        # IF NODE CANNOT BE PLACED IN TREE, ORPHAN IT UNTIL ITS PARENT IS FOUND
        if not root.find_parent_of(node):
            orphans.append(node)
    # logger.info('{} orphaned tweets for conversation {} before resolution'.format(len(orphans), submission.id))
    orphan_added = True
    while orphan_added:
        orphan_added, orphans = solve_orphans(orphans, root)
    if len(orphans) > 0:
        logger.error('{} orphaned tweets for conversation {}'.format(len(orphans), submission.fullname))
        logger.error('{} downloaded tweets'.format(len(comments)))
    return root


def sort_comments_for_db(submission):
    submission.comments.replace_more(limit=None)
    result = []
    for comment in submission.comments.list():
        result.append(comment)
    if len(result) > 3:
        pass
    result.sort(key=lambda x: x.created)

    return result


def convert_time_stamp_to_django(comment):
    created_time = datetime.datetime.fromtimestamp(comment.created_utc)
    amsterdam_timezone = pytz.timezone('Europe/Berlin')
    created_time = amsterdam_timezone.localize(created_time)
    return created_time


def compute_author_id(comment):
    name = "[deleted]"
    if comment.author is not None:
        if hasattr(comment.author, "name"):
            name = comment.author.name
    author_id = convert_to_hash(name)
    return author_id, name
