from delab.models import SimpleRequest, TwTopic


def apply_tweet_filter(tweet, tweet_filter):
    if tweet_filter is not None:
        tweet = tweet_filter(tweet)
        # the idea here is that the filter may have to save the tweet to create foreign keys
        # in this case the save method will fail because of an integrity error
        if tweet.pk is None:
            tweet.save()

    else:
        tweet.save()
