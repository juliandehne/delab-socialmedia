import itertools
import logging

import networkx as nx

from delab.models import Tweet, ConversationFlow
from delab.network.conversation_network import get_nx_conversation_graph, get_root

logger = logging.getLogger(__name__)


def get_all_reply_paths(conversation_id, min_path_length, required_max_path_length):
    G = get_nx_conversation_graph(conversation_id)
    all_paths = []
    nodes_combs = itertools.combinations(G.nodes, 2)
    for source, target in nodes_combs:
        paths = nx.all_simple_paths(G, source=source, target=target, cutoff=required_max_path_length)

        for path in paths:
            if path not in all_paths and path[::-1] not in all_paths and len(path) >= min_path_length:
                all_paths.append(path)
    return all_paths


def get_path(twitter_id, conversation_id, min_path_length=3, required_max_path_length=4):
    paths = get_all_reply_paths(conversation_id, min_path_length, required_max_path_length)
    current_best_path_index = None
    current_best_score = 0
    index_count = 0
    for path in paths:
        if twitter_id in path:
            p_index = path.index(twitter_id)
            previous_tweets = p_index
            following_tweets = len(path) - p_index - 1
            middleness_score = min(previous_tweets, following_tweets) - abs(previous_tweets - following_tweets)
            if middleness_score > current_best_score:
                current_best_path_index = index_count
            current_best_score = max(current_best_score, middleness_score)
        index_count += 1
    if current_best_path_index is None:
        return None
    return paths[current_best_path_index]


def get_conversation_flows(conversation_id, only_text=False):
    reply_tree = get_nx_conversation_graph(conversation_id)
    root = get_root(reply_tree)
    leaves = [x for x in reply_tree.nodes() if reply_tree.out_degree(x) == 0]
    flows = []
    flow_dict = {}
    for leaf in leaves:
        paths = nx.all_simple_paths(reply_tree, root, leaf)
        flows.append(next(paths))
    for flow in flows:
        flow_name = str(flow[0]) + "_" + str(flow[-1])
        if only_text:
            flow_tweets = list(
                Tweet.objects.filter(twitter_id__in=flow).order_by("created_at").values_list("text", flat=True))
        else:
            flow_tweets = Tweet.objects.filter(twitter_id__in=flow).order_by("created_at").all()
        flow_dict[flow_name] = flow_tweets

    name_of_longest = max(flow_dict, key=lambda x: len(set(flow_dict[x])))
    return flow_dict, name_of_longest


def compute_conversation_flows(conversation_id):
    """
    computes the conversation flows for all conversations in order to be used by the api (flows export)
    or by the dual flow analysis
    @param conversation_id:
    """
    if not ConversationFlow.objects.filter(conversation_id=conversation_id).exists():
        try:
            flows, name_of_longest = get_conversation_flows(conversation_id)
            for name, tweets in flows.items():
                flow, created = ConversationFlow.objects.get_or_create(
                    flow_name=name,
                    conversation_id=conversation_id,
                    longest=(name == name_of_longest)
                )
                flow.tweets.add(*tweets)
        except AssertionError as ae:
            logger.error(ae)
