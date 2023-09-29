import logging

from api_settings import *
from delab_trees.delab_tree import DelabTree
from models.platform import PLATFORM

logger = logging.getLogger(__name__)


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
