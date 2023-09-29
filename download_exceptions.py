class ConversationNotInRangeException(Exception):
    """Exception raised for errors in the input salary.

    Attributes:
        salary -- input salary which caused the error
        message -- explanation of the error
    """

    def __init__(self, conversation_size, message="Conversation downloaded is not in range"):
        self.conversation_size = conversation_size
        self.message = message
        super().__init__(self.message)


class NoDailySubredditAvailableException(Exception):
    """Exception raised for errors in the input salary.

    Attributes:
        salary -- input salary which caused the error
        message -- explanation of the error
    """

    def __init__(self, language, message="There are no more daily subreddits available"):
        self.language = language
        self.message = message
        super().__init__(self.message)


class NoDailyMTHashtagsAvailableException(Exception):
    """Exception raised for errors in the input salary.

    Attributes:
        salary -- input salary which caused the error
        message -- explanation of the error
    """

    def __init__(self, language, message="There are no more daily mastodon hashtags available"):
        self.language = language
        self.message = message
        super().__init__(self.message)