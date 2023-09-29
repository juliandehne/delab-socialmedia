"""
 use this endpoint (also in magic https) GET https://api.twitter.com/2/users/253257813?user.fields=location,username,name
"""
import logging
import time

from django.db import IntegrityError
from django.db.models import Q


logger = logging.getLogger(__name__)


def download_authors(author_ids):
    twarc = DelabTwarc()
    new_authors = repair_fk_tweet_authors(author_ids)
    # batch processing means breaking down a big loop
    # in smaller loops
    author_batches = batch(new_authors, 99)
    for author_batch in author_batches:
        download_user_batch(author_batch, twarc)


def repair_fk_tweet_authors(author_ids):
    """
         Historically tweets and authors were not linked by foreign key
         This creates the foreign key association.
         In fact, it may be worth testing if this method is still needed. It looks obsolete!!
         @param author_ids: the list of ids that need to be downloaded
         @return:the list were authors are appended to, that are not yet in the author table
    """
    new_authors = []
    for author_id in author_ids:

        try:
            author = TweetAuthor.objects.get(twitter_id=author_id)
        except TweetAuthor.DoesNotExist:
            author = None
        if author:
            tweets = Tweet.objects.filter(author_id=author_id).all()
            for tweet in tweets:
                tweet.tw_author = author
                tweet.save(update_fields=["tw_author"])
        else:
            new_authors.append(author_id)
    new_authors = list(set(new_authors))
    return new_authors


def download_user_batch(author_batch, twarc):
    """
    Utility function to download all the author data in a batch (in chunks) using the twitter api existing for that reason
    :param author_batch:
    :param twarc:
    :return:
    """
    # downloads the author data like names
    users = twarc.user_lookup(users=author_batch)

    for user_batch in users:
        if "data" in user_batch:
            # iterate through the author data
            for author_payload in user_batch["data"]:
                user_obj = author_payload
                try:
                    author_id = user_obj["id"]
                    tweets = Tweet.objects.filter(author_id=author_id).all()
                    author, created = TweetAuthor.objects.get_or_create(
                        twitter_id=user_obj["id"],
                        name=user_obj["name"],
                        screen_name=user_obj["username"],
                        location=user_obj.get("location", "unknown"),
                        followers_count=user_obj["public_metrics"]["followers_count"],
                        # tweet=tweet
                    )
                    author.full_clean()
                    for tweet in tweets:
                        tweet.tw_author = author
                        tweet.save(update_fields=["tw_author"])

                except IntegrityError:
                    logger.error("author already exists")

                # except Exception as e:
                # Normally the API does this, too
                # This kind of error handling is very dangerous and thus commented out
                # traceback.print_exc()
                #    logger.info(
                #        "############# Exception: Rate limit was exceeded while downloading author info." +
                #        " Going to sleep for 15!")
                #    time.sleep(15 * 60)
        else:
            if "errors" in user_batch:
                deal_with_missing_authors(author_batch, twarc, user_batch)


def deal_with_missing_authors(author_batch, twarc, userbatch):
    """
    create fake users for those that could not be downloaded
    """
    author_batch_size = len(author_batch)
    errors = userbatch["errors"]
    for error in errors:
        if "value" in error:
            user_not_found = int(error["value"])
            author_batch.remove(user_not_found)
            # create stand_in_user
            tweets = Tweet.objects.filter(author_id=user_not_found).all()
            author, created = TweetAuthor.objects.get_or_create(
                twitter_id=user_not_found,
                name="user_deleted",
                screen_name="user_deleted",
                # tweet=tweet
            )
            author.full_clean()
            for tweet in tweets:
                tweet.tw_author = author
                tweet.save(update_fields=["tw_author"])
    if len(author_batch) < author_batch_size:
        download_user_batch(author_batch, twarc)









