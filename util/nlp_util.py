import string
import re


def process_tweet(tweet):
    from nltk.tokenize import TweetTokenizer
    from nltk.stem import PorterStemmer
    from nltk.corpus import stopwords

    """
    Input:
        tweet: a string containing a tweet
        This was downloaded from the coursera project on NLP https://www.coursera.org/learn/sequence-models-in-nlp/programming/DyPCv/sentiment-with-deep-neural-networks/lab
    Output:
        tweets_clean: a list of words containing the processed tweet

    """

    tweet_tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)

    # Stop words are messy and not that compelling;
    # "very" and "not" are considered stop words, but they are obviously expressing sentiment

    # The porter stemmer lemmatizes "was" to "wa".  Seriously???

    # I'm not sure we want to get into stop words
    stopwords_english = stopwords.words('english')

    # Also have my doubts about stemming...
    stemmer = PorterStemmer()

    # remove stock market tickers like $GE
    tweet = re.sub(r'\$\w*', '', tweet)
    # remove old style retweet text "RT"
    tweet = re.sub(r'^RT[\s]+', '', tweet)
    # remove hyperlinks
    tweet = re.sub(r'https?:\/\/.*[\r\n]*', '', tweet)
    # remove hashtags
    # only removing the hash # sign from the word
    tweet = re.sub(r'#', '', tweet)
    # tokenize tweets
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)
    tweet_tokens = tokenizer.tokenize(tweet)

    tweets_clean = []
    for word in tweet_tokens:
        if (word not in stopwords_english and  # remove stopwords
                word not in string.punctuation):  # remove punctuation
            # tweets_clean.append(word)
            stem_word = stemmer.stem(word)  # stemming word
            tweets_clean.append(stem_word)

    return tweets_clean
