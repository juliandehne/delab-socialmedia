import datetime
import logging
from time import sleep
from pytz.exceptions import AmbiguousTimeError
from delab.models import TweetAuthor, Timeline
from delab.delab_enums import PLATFORM
from delab.tw_connection_util import get_praw
from util.abusing_strings import convert_to_hash

logger = logging.getLogger(__name__)


def download_timelines_reddit(simple_request_id=-1):
    reddit = get_praw()
    if simple_request_id < 0:
        authors = TweetAuthor.objects.filter(platform=PLATFORM.REDDIT, has_timeline__isnull=True).all()
    else:
        authors = TweetAuthor.objects.filter(platform=PLATFORM.REDDIT, tweet__simple_request_id=simple_request_id,
                                             has_timeline__isnull=True).all()

    n_authors = len(authors)
    a_counter = 0
    for author in authors:
        try:
            a_counter += 1
            reddit_name = author.name
            redditor2 = reddit.redditor(reddit_name)
            comments = redditor2.comments.new(limit=5)
            for comment in comments:
                created_time = datetime.datetime.fromtimestamp(comment.created)
                try:
                    timeline, created = Timeline.objects.get_or_create(
                        author_id=author.twitter_id,
                        tweet_id=convert_to_hash(comment.id),
                        text=comment.body,
                        lang=comment.subreddit.lang,
                        platform=PLATFORM.REDDIT,
                        conversation_id=convert_to_hash(comment.submission.id),
                        created_at=created_time,
                        tw_author=author
                    )
                    author.has_timeline = created
                except AmbiguousTimeError:
                    print("could not store time to db {}".format(created_time))
                    author.has_timeline = False
                logger.debug("downloaded timeline {}/{} authors".format(a_counter, n_authors))
        except Exception:
            sleep(15)
            author.has_timeline = False
    TweetAuthor.objects.bulk_update(authors, fields=["has_timeline"])
