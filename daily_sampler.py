import logging

from api_settings import *
from datasource.mastodon.download_daily_political_sample_mstd import MTSampler
from datasource.reddit.download_daily_political_rd_sample import RD_Sampler
from datasource.twitter.download_daily_political_sample import download_daily_political_sample
from delab_trees import TreeManager
from delab_trees.delab_post import DelabPost
from delab_trees.delab_tree import DelabTree
from models.language import LANGUAGE
from models.platform import PLATFORM

logger = logging.getLogger(__name__)


def download_samples(platform, min_results, language, connector) -> list[list[DelabPost]]:
    result = []
    flow_result_count = 0
    while flow_result_count < min_results:
        downloaded_trees = download_daily_sample(platform=platform, language=language, connector=connector)
        validated_trees = validate_trees(downloaded_trees, platform)

        if len(validated_trees) > 0:
            forest = TreeManager.from_trees(validated_trees)

            flow_sample: list[list[DelabPost]] = forest.get_flow_sample(5, filter_function=meta_list_filter)
            if flow_sample is not None and len(flow_sample) > 0:
                logging.debug("found flows {}".format(len(flow_sample)))
                result += flow_sample

                # collect ids of the trees from the sample
                sample_tree_ids = []
                for sample in flow_sample:
                    first_post = sample[0]
                    tree_id = first_post.tree_id
                    sample_tree_ids.append(tree_id)
                # throw out the trees not sampled
                forest.keep(sample_tree_ids)

                flow_result_count = len(result)

    return result


def download_daily_sample(platform: PLATFORM,
                          language=LANGUAGE.ENGLISH,
                          max_results=MT_STUDY_DAILY_FLOWS_NEEDED,
                          connector=None) -> list[DelabTree]:
    """
    @param platform:
    @param language:
    @param connector:
    @param max_results: the maximum number of suitable trees to be found for a given platform and a day
    @return:
    """
    if platform == PLATFORM.TWITTER:
        return download_daily_political_sample(language, connector)
    if platform == PLATFORM.REDDIT:
        sampler = RD_Sampler(language)
        return sampler.download_daily_rd_sample(max_results, connector)
    if platform == PLATFORM.MASTODON:
        sampler = MTSampler(language=language)
        return sampler.download_daily_political_sample_mstd(connector)
    else:
        raise NotImplementedError()


def validate_trees(downloaded_trees, platform):
    validated_trees = []
    # reddit sampler has integrated validation
    if platform != PLATFORM.REDDIT:
        for tree in downloaded_trees:
            validated = tree.validate(verbose=False)
            useful = check_general_tree_requirements(tree, platform=platform)
            if validated and useful:
                validated_trees.append(tree)
    else:
        validated_trees = downloaded_trees
    return validated_trees


def check_general_tree_requirements(delab_tree: DelabTree, verbose=False, platform=PLATFORM.REDDIT):
    if delab_tree is not None:
        tree_size = delab_tree.total_number_of_posts()
        tree_depth = delab_tree.depth()
        min_conversation_length = MIN_CONVERSATION_LENGTH
        min_depth = MIN_CONVERSATION_DEPTH
        max_conversation_length = MAX_CONVERSATION_LENGTH
        if platform == PLATFORM.REDDIT:
            max_conversation_length = MAX_CONVERSATION_LENGTH_REDDIT
        if platform == PLATFORM.MASTODON:
            min_conversation_length = MIN_CONVERSATION_LENGTH_MASTODON
            min_depth = MIN_CONVERSATION_DEPTH_MASTODON
        if min_conversation_length < tree_size < max_conversation_length and tree_depth >= min_depth:
            if verbose:
                logger.debug("found suitable conversation with length {} and depth {}".format(tree_size, tree_depth))
            return True
        return False
    else:
        if verbose:
            logger.error("could not check tree requirements for NoneType")
        return False


def is_short_text(text):
    """
    Check if the given text is shorter than 280 characters.

    Args:
        text (str): The text to be checked.

    Returns:
        bool: True if the text is shorter than 280 characters, False otherwise.
    """
    return len(text) < 500


def is_bad_reddit_case(text):
    return "[removed]" not in text and "[entfernt]" not in text and "!approve" not in text and "!ban" not in text


def meta_list_filter(posts: list[DelabPost]):
    return all([meta_filter(x) for x in posts]) and filter_self_answers(posts)


def meta_filter(post: DelabPost):
    text = post.text
    is_short = is_short_text(text)
    is_bad_rd = is_bad_reddit_case(text)
    result = is_short and is_bad_rd
    return result


def filter_self_answers(posts: list[DelabPost]):
    # posts = posts.sort(key=lambda x: x.created_at)
    previous_author = None
    for post in posts:
        if post.author_id == previous_author:
            return False
        previous_author = post.author_id
    return True
