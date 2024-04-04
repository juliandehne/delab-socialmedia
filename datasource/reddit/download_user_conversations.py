import praw

from connection_util import get_praw
from datetime import datetime, timezone

from datasource.reddit.download_conversations_reddit import compute_reddit_tree
from delab_trees.delab_tree import DelabTree


def get_user_conversations(username, start_date=None, reddit=None):
    if reddit is None:
        reddit = get_praw()

    if start_date is None:
        # Define the start date for fetching comments and submissions
        start_date = datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()

    user = reddit.redditor(username)

    submissions = set()
    # Fetch submissions from the user's comments
    for comment in user.comments.new(limit=None):  # Adjust limit as necessary
        if comment.created_utc >= start_date:
            submission = reddit.submission(id=comment.link_id.split('_')[1])
            submissions.add(submission)

        # Fetch submissions directly made by the user
    for submission in user.submissions.new(limit=None):  # Adjust limit as necessary
        if submission.created_utc >= start_date:
            submissions.add(submission)

    trees = []
    for submission in submissions:
        trees.append(compute_reddit_tree(submission))

    trees_delab_format = [DelabTree.from_recursive_tree(x) for x in trees]
    return trees_delab_format
