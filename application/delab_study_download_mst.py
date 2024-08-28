import os

from dotenv import load_dotenv
import pandas as pd

from models.platform import PLATFORM
from socialmedia import get_conversations_by_user

load_dotenv()

# %%
mst_user = os.environ.get("mst_username")
mst_conversations = get_conversations_by_user(mst_user, PLATFORM.MASTODON)
assert all(x.validate() for x in mst_conversations)
df_mastodon = pd.concat([x.df for x in mst_conversations])
df_mastodon["platform"] = PLATFORM.MASTODON

# %%

# reddit_user = os.environ.get("reddit_user_name")
# reddit_conversations = get_conversations_by_user(reddit_user, PLATFORM.REDDIT)
# assert all(x.validate(check_time_stamps_differ=False) for x in reddit_conversations) # automoderator produce identical timestamps
# df_reddit = pd.concat([x.df for x in reddit_conversations])
# df_reddit["platform"] = PLATFORM.REDDIT

# %%
# df_all = pd.concat([df_mastodon, df_reddit])

# %%
df_mastodon.to_csv("delab_study2_mst.csv")
