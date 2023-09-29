import logging

from django.db.models import Exists, OuterRef
from delab.models import Timeline, Tweet, TweetAuthor
from delab.delab_enums import PLATFORM
from delab.tw_connection_util import DelabTwarc
from django.db import IntegrityError

logger = logging.getLogger(__name__)


def update_timelines_twitter(simple_request_id=-1,
                             fix_legacy_db=False):
    """
    This downloads the timelines for a given conversation
    :param simple_request_id: the request id that was used when querying the conversation
    :param fix_legacy_db: Download the authors for the conversation, that are still missing in the database
    :return:
    """
    if simple_request_id < 0:
        author_ids = TweetAuthor.objects.filter(has_timeline__isnull=True, platform=PLATFORM.TWITTER).values_list(
            'twitter_id',
            flat=True)
        if fix_legacy_db:
            fix_legacy()
    else:
        author_ids = Tweet.objects.filter(~Exists(Timeline.objects.filter(author_id=OuterRef("author_id")))).filter(
            simple_request_id=simple_request_id, platform=PLATFORM.TWITTER).values_list('author_id', flat=True).distinct()
    get_user_timeline_twarc(author_ids)


def save_author_tweet_to_tb(json_result, author_id):
    """
    This saves single tweets from the timeline to the database
    :param json_result: the timeline result payload
    :param author_id: the twitter id of timeline's owner
    :return:
    """
    if "data" in json_result:
        data = json_result["data"]
        for tweet_dict in data:
            in_reply_to_user_id = tweet_dict.get("in_reply_to_user_id", None)

            t, created = Timeline.objects.get_or_create(
                text=tweet_dict["text"],
                created_at=tweet_dict["created_at"],
                author_id=author_id,
                tweet_id=tweet_dict["id"],
                conversation_id=tweet_dict["conversation_id"],
                in_reply_to_user_id=in_reply_to_user_id,
                lang=tweet_dict["lang"]
            )
            t.full_clean()
            try:
                author = TweetAuthor.objects.filter(twitter_id=t.author_id).get()
                author.has_timeline = True
                author.save(update_fields=["has_timeline"])
                t.tw_author = author
                t.save(update_fields=["tw_author"])
            except IntegrityError as ie:
                logger.error(ie)
                pass
            except Exception as not_clear:
                logger.error("author for tweet was not downloaded + {}".format(not_clear))
    else:
        logger.debug("no timeline was found for author {}".format(author_id))


def fix_legacy():
    """
    Download missing authors and link them with existing timeline elements
    This method only provides database consistency if the following order is not kept:
    1. download conversations
    2. download authors
    3. download timelines
    :return:
    """
    # update authors "has_timeline" field
    authors = TweetAuthor.objects.filter(has_timeline__isnull=True)
    authors_ids = authors.values_list('twitter_id', flat=True)
    existing_timelines = Timeline.objects.filter(author_id__in=authors_ids).select_related("tw_author")
    for existing_timeline in existing_timelines:
        try:
            author = existing_timeline.tw_author
            if author is None:
                author = TweetAuthor.objects.filter(twitter_id=existing_timeline.author_id).get()
                # author = authors.filter(twitter_id=existing_timeline.author_id).get()
            author.has_timeline = True
            author.save(update_fields=["has_timeline"])
            existing_timeline.tw_author = author
            existing_timeline.save(update_fields=["tw_author"])
        except Exception:
            logger.error("not all authors have been downloaded prior to timeline downloads")
    # update timelines that did not store the associated author object
    existing_timelines2 = Timeline.objects.filter(tw_author__isnull=True).all()
    for existing_timeline2 in existing_timelines2:
        try:
            author = TweetAuthor.objects.get(twitter_id=existing_timeline2.author_id)
            existing_timeline2.tw_author = author
            existing_timeline2.save(update_fields=["tw_author"])
        except TweetAuthor.DoesNotExist:
            logger.error("author was not downloaded before updating timelines")


def get_user_timeline_twarc(author_ids, max_results=10):
    """
    get user timelines on a batch basis
    :param author_ids: [author_id1, author_id2, ...]
    :param max_results: int (the number of elements from the timeline
    :return:
    """
    twarc_connector = DelabTwarc()

    author_count = 0
    for author_id in author_ids:
        author_count += 1
        logger.debug("computed {}/{} of the timelines".format(author_count, len(author_ids)))
        count = 0
        tweets = twarc_connector.timeline(user=author_id, max_results=min(max_results, 64), exclude_retweets=True,
                                          exclude_replies=True)
        for tweet in tweets:
            count += 1
            if count > max_results:
                break
            save_author_tweet_to_tb(tweet, author_id)
        if count == 0:
            try:
                logger.debug("could not find a timeline for the give")
                author = TweetAuthor.objects.filter(twitter_id=author_id).get()
                author.has_timeline = False
                author.save(update_fields=["has_timeline"])
            except TweetAuthor.DoesNotExist:
                if author_id is None:
                    raise ValueError("there is a programming error in the timeline download")
                else:
                    logger.error("could not set timeline to false for author_id: {}".format(author_id))
