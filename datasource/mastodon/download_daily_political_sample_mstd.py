import logging
from copy import deepcopy
from random import choice
from delab.corpus.download_exceptions import NoDailyMTHashtagsAvailableException
from delab.corpus.mastodon.download_conversations_mastodon import download_conversations_to_search
from delab.tw_connection_util import create_mastodon
from datetime import datetime, timedelta
from delab.delab_enums import LANGUAGE

logger = logging.getLogger(__name__)


class MTSampler:
    # Hashtags überarbetien/aussortieren
    hashtags = [
        'politics',
        'currentaffairs',
        'news',
        'government',
        'worldnews',
        'elections',
        'voting',
        'democracy',
        'activism',
        'publicpolicy',
        'leadership',
        'policy',
        'socialjustice',
        'humanrights',
        'climatechange',
        'equality',
        'immigration',
        'healthcare',
        'education',
        'technology',
        'environment',
        'sustainability',
        'corruption',
        'protest',
        'conservative',
        'liberal',
        'republicans',
        'democrats',
        'police',
        'guncontrol',
        'voterregistration',
        'civilrights',
        'womensrights',
        'racialjustice',
        'disabilityrights',
        'incomeinequality',
        'taxreform',
        'foreignrelations',
        'nationalsecurity',
        'waronterror',
        'peacebuilding',
        'diplomacy',
        'globalgovernance',
        'freespeech',
        'fakenews',
        'factchecking',
        'media',
        'journalism',
        'transparency',
        'lobbying',
        'electionfraud',
        'foreigninterference',
        'votingrights',
        'electoralreform',
        'politicalasylum',
        'refugeecrisis',
        'bordersecurity',
        'cleanenergy',
        'sustainabledevelopment',
        'educationreform',
        'edtech',
        'healthcarereform',
        'mentalhealth',
        'pandemicresponse',
        'technologyethics',
        'dataprivacy',
        'surveillance',
        'cybersecurity',
        'environmentaljustice',
        'ecology',
        'sustainableliving',
        'corporategovernance',
        'ethics',
        'consumerprotection',
        'urbanplanning',
        'rurallife',
        'BLM',
        'balcklivesmatter',
        'pride',
        'LBTQIA+',
        'translivesmatter',
        'transphobia',
        'accountability',
        'queer',
        'LGBT',
        'BlackTransLivesMatter',
        'AffordableHousing',
        "Progressive",
        "SocialJustice",
        "TaxTheRich",
        "MedicareForAll",
        "ClimateAction",
        "BlackLivesMatter",
        "FightFor15",
        "GreenNewDeal",
        "UniversalHealthcare",
        "DefundThePolice",
        "LGBTQRights",
        "EqualityForAll",
        "WorkersRights",
        "FreeCollege",
        "WomenEmpowerment",
        "IncomeInequality",
        "EnvironmentalJustice",
        "AbolishICE",
        "ReproductiveRights",
        "EndCorporateGreed",
        'refugeeswelcome',
        "Conservative",
        "MAGA",
        "ProLife",
        "2A",
        "TaxReform",
        "BorderSecurity",
        "FreeMarket",
        "SchoolChoice",
        "TraditionalValues",
        "ReligiousFreedom",
        "NationalSecurity",
        "DefendThePolice",
        "LowerTaxes",
        "LimitedGovernment",
        "AmericaFirst",
        "IndividualLiberty",
        "RightToBearArms",
        "ProFamily",
        "EconomicFreedom",
        "Capitalism"]

    german_hashtags = [
        "Politik",
        "Bundestagswahl",
        "Bundestag",
        "Bundesregierung",
        "Wahlen",
        "Parteien",
        "Demokratie",
        "Europa",
        "Klimapolitik",
        "Flüchtlinge",
        "Asyl",
        "Corona",
        "Gesundheitspolitik",
        "Wirtschaftspolitik",
        "Arbeit",
        "Soziales",
        "Umwelt",
        "Bildung",
        "Digitalisierung",
        "Kriminalität",
        "Sicherheit",
        "Familienpolitik",
        "Energiepolitik",
        "Landwirtschaft",
        "Steuern",
        "Außenpolitik",
        "Integration",
        "Gleichberechtigung",
        "Grundrechte",
        "Meinungsfreiheit",
        "Linke",
        "SPD",
        "DieGrünen",
        "Sozialdemokratie",
        "Antikapitalismus",
        "Solidarität",
        "Gerechtigkeit",
        "Arbeitnehmerrechte",
        "Gleichheit",
        "Umweltschutz",
        "Klimagerechtigkeit",
        "Feminismus",
        "Bildungsgerechtigkeit",
        "Sozialstaat",
        "BGE",
        "Antirassismus",
        "Asylrecht",
        "Demokratie",
        "Frieden",
        "Linksjugend",
        "SolidarischeÖkonomie",
        "Mindestlohn",
        "Kollektiveigentum",
        "Antifa",
        "KampfgegenRechts",
        "Protest",
        "Globalisierungskritik",
        "Widerstand",
        "Bündnis90DieGrünen",
        "Ökosozialismus",
        "CDU",
        "CSU",
        "AfD",
        "Konservativ",
        "Werte",
        "Heimat",
        "Familie",
        "Religion",
        "Tradition",
        "Sicherheit",
        "Nationalismus",
        "Patriotismus",
        "GesetzundOrdnung",
        "Marktwirtschaft",
        "Freiheit",
        "Migrationspolitik",
        "Asylpolitik",
        "Grenzschutz",
        "Bildung",
        "Wirtschaftswachstum",
        "Steuersenkungen",
        "Arbeitsmarkt",
        "Energiewende",
        "Klimaskeptiker"
    ]

    daily_en_hashtags = {}
    daily_de_hashtags = {}

    def __init__(self, language):
        current_date, hashtag_string = pick_random_hashtag(language)
        self.hashtag_string = hashtag_string
        logger.debug(
            "current hashtags to search are en: {}, de: {}".format(len(MTSampler.daily_en_hashtags[current_date]),
                                                                   len(MTSampler.daily_de_hashtags[current_date])))
        self.language = language
        self.current_date = current_date

    def download_daily_political_sample_mstd(self, topic_string):
        hashtag = self.hashtag_string
        # toots in the last 24 hours
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        mastodon = create_mastodon()
        #logger.debug("searching for conversations for hashtag {}".format(self.hashtag_string))
        downloaded_trees = download_conversations_to_search(query=hashtag, mastodon=mastodon, topic=topic_string,
                                                            since=yesterday, daily_sample=True)

        logger.debug("returning {} conversations for hashtags {}, {} hashtags not searched for lang {}"
                     .format(len(downloaded_trees), self.hashtag_string,
                             len(self.get_current_dictionary()), self.language))

        return downloaded_trees

    def get_current_dictionary(self):
        if self.language == LANGUAGE.ENGLISH:
            return self.daily_en_hashtags[self.current_date]
        else:
            return self.daily_de_hashtags[self.current_date]


def pick_random_hashtag(language):
    current_date = datetime.now().date()
    # init dictionary of subreddits if empty
    if current_date not in MTSampler.daily_en_hashtags:
        MTSampler.daily_en_hashtags[current_date] = deepcopy(MTSampler.hashtags)
    if current_date not in MTSampler.daily_de_hashtags:
        MTSampler.daily_de_hashtags[current_date] = deepcopy(MTSampler.german_hashtags)
    # get current state of available subreddits and pick one
    if language == LANGUAGE.GERMAN:
        available_hashtags = MTSampler.daily_de_hashtags[current_date]
    else:
        available_hashtags = MTSampler.daily_en_hashtags[current_date]
    if len(available_hashtags) == 0:
        raise NoDailyMTHashtagsAvailableException(language=language)
    else:
        hashtag_string = choice(available_hashtags)
    # update dictionary with current available states
    if language == LANGUAGE.ENGLISH:
        current_list = deepcopy(MTSampler.daily_en_hashtags[current_date])
        current_list.remove(hashtag_string)
        MTSampler.daily_en_hashtags[current_date] = current_list
    else:
        current_list = deepcopy(MTSampler.daily_de_hashtags[current_date])
        current_list.remove(hashtag_string)
        MTSampler.daily_de_hashtags[current_date] = current_list
    return current_date, hashtag_string
