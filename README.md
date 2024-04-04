# delab-socialmedia
A python library to facilitate downloading conversation trees from social media platforms like Twitter, Reddit or Mastodon.

## Overview

This library provides a unified interface for downloading conversations from Twitter, Reddit, and Mastodon. It simplifies the process of querying and retrieving conversations based on specific criteria, such as language, query string, and the recency of conversations. The library is designed to work with connectors specific to each platform, such as `twarc` for Twitter, `praw` for Reddit, and the Mastodon API for Mastodon.

## Features

- Download conversations from Twitter, Reddit, and Mastodon using a unified interface.
- Filter conversations by language, query string, and recency.
- Download the conversations in a unified format DelabTree that allows CSS analysis for the conversation trees, but also exports as pandas dataframe or networkx graph
- Validate and filter the downloaded conversations based on custom criteria.
- Download a daily sample of political discussions for CSS research
- NOTE: Twitter functions have not been tested since its demise

## Installation

```bash
pip install delab-socialmedia
```

## Getting Started

Before you begin, make sure you have the necessary credentials:
- For Reddit: `reddit_secret`, `reddit_script_id`, `reddit_user`, `reddit_password`, `user_agent`
- For Mastodon: `client_id`, `client_secret`, `access_token`

       
        
### Reddit Connector (`get_praw` function)

This function creates a connector for Reddit using the `praw` library. 

#### Direct Variable Setting

```python
from connection_util import get_praw

reddit = get_praw(
    reddit_secret='YOUR_REDDIT_SECRET',
    reddit_script_id='YOUR_REDDIT_SCRIPT_ID',
    reddit_user='YOUR_REDDIT_USERNAME',
    reddit_password='YOUR_REDDIT_PASSWORD',
    user_agent='YOUR_USER_AGENT'
)
```

#### Use YAML

```yaml
reddit_secret: YOUR_REDDIT_SECRET
reddit_script_id: YOUR_REDDIT_SCRIPT_ID
reddit_user: YOUR_REDDIT_USERNAME
reddit_password: YOUR_REDDIT_PASSWORD
user_agent: YOUR_USER_AGENT
```

and

```python
from connection_util import get_praw
reddit = get_praw(
    use_yaml=True,
    yaml_path='path/to/your/social_media_credentials.yml'
)
```

### Mastodon Connector (`create_mastodon` function)

#### Direct Variable Setting

```python
from connection_util import create_mastodon

mastodon = create_mastodon(
    client_id='YOUR_CLIENT_ID',
    client_secret='YOUR_CLIENT_SECRET',
    access_token='YOUR_ACCESS_TOKEN',
    api_base_url='https://mastodon.social/'
)
```

#### Use YAML

```yaml
client_id: YOUR_CLIENT_ID
client_secret: YOUR_CLIENT_SECRET
access_token: YOUR_ACCESS_TOKEN
api_base_url: https://mastodon.social/
```

and 

```python
from connection_util import create_mastodon

mastodon = create_mastodon(
    use_yaml=True,
    yaml_path='path/to/your/social_media_credentials.yml'
)
```


### Twitter Connector 

Analogously, for Twitter, although the Twitter access has not been tested.  

```python
from twarc import Twarc2

class DelabTwarc(Twarc2):
    def __init__(self, access_token=None, access_token_secret=None, bearer_token=None, consumer_key=None,
                 consumer_secret=None, use_yaml=False, yaml_path=None):
        """
        create the Twitter connector
        :param access_token:
        :param access_token_secret:
        :param bearer_token:
        :param consumer_key:
        :param consumer_secret:
        :param use_yaml:
        :param yaml_path:
        """
        ...

```

## Download daily sample

```python
from models.language import LANGUAGE
from models.platform import PLATFORM
from connection_util import create_mastodon
from socialmedia import download_daily_sample_conversations

connector = create_mastodon()
conversations = download_daily_sample_conversations(platform=PLATFORM.MASTODON,
                                                    language=LANGUAGE.ENGLISH,
                                                    min_results=5, 
                                                    connector=connector)
```


### Download Conversations

The library provides functions to download conversations based on various parameters. Here's an example of downloading conversations from Reddit:

```python
from socialmedia import download_conversations, PLATFORM, LANGUAGE
from connection_util import get_praw

connector = get_praw()
conversations = download_conversations(query_string="Politics",
                                       platform=PLATFORM.REDDIT,
                                       language=LANGUAGE.ENGLISH,
                                       recent=True,
                                       max_conversations=30,
                                       connector=connector)
```



### Get Conversations by User

To get all conversations a given user has participated in on Reddit or Mastodon:

```python
from socialmedia import get_conversations_by_user, PLATFORM

from connection_util import get_praw

connector = get_praw()
user_conversations = get_conversations_by_user(username="u/example_user",
                                                platform=PLATFORM.REDDIT,
                                                connector=connector)
```

## Contribution

Contributions to improve the library are welcome. Please submit pull requests or open issues to suggest changes or report bugs.

## License

MIT

