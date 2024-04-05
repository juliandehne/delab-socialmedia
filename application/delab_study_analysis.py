import os

import pandas as pd
from dotenv import load_dotenv
from delab_trees.main import TreeManager

load_dotenv()

# %%

reddit_user = os.environ.get("study_user_name")
print(reddit_user)

# %%

df = pd.read_csv("delab_study2.csv")

df_calms = df[df["tw_author__name"] == reddit_user]
print(df_calms.shape)
author_ids = set(df_calms.author_id)

# %%
df["post_id"] = df["post_id"].astype(str)
df["parent_id"] = df["parent_id"].astype(str)

manager = TreeManager(df)

flow_lists = []
for tree_id, tree in manager.trees.items():
    flows = tree.get_conversation_flows()
    flow_lists.append(list(flows[0].values()))

# %%
# Flatten the list of lists into a single list
flat_list = [item for sublist in flow_lists for item in sublist]

# %%
flows_with_calm = []
for path in flat_list:
    contained = any(post.author_id in author_ids for post in path)
    if contained:
        flows_with_calm.append(path)

# %%
n_follows = []
lengths = []
positions = []
for path in flows_with_calm:
    length = len(path)
    position = None
    positions.append(position)
    count = 0
    for post in path:
        count += 1
        if post.author_id in author_ids:
            position = count
    if position is None:
        raise Exception
    lengths.append(length)
    n_follows.append(length-position)


follow_df = pd.DataFrame()
follow_df["lengths"] = lengths
follow_df["positions"] = positions
follow_df["n_follows"] = n_follows
print(follow_df.n_follows.value_counts())