import pickle

import prawcore
from dotenv import load_dotenv

from models.platform import PLATFORM
# Reading the list from the file
from socialmedia import get_conversations_by_conversation_url

load_dotenv()

with open('reddit_urls.pkl', 'rb') as file:
    reddit_urls = pickle.load(file)

trees = []

for url in reddit_urls:
    try:
        tree = get_conversations_by_conversation_url(url, platform=PLATFORM.REDDIT)
        trees.append(tree)
    except prawcore.exceptions.Forbidden as ex:
        pass

with open('intervention_conversations.pkl', 'wb') as file:
    pickle.dump(trees, file)
