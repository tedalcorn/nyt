"""Extract obituary records from raw Archive API dumps and write data/obituaries.json."""
import json, os, re, glob, sys, unicodedata
from collections import Counter

RAW_DIR = 'data/raw'
OUT_PATH = 'data/obituaries.json'

# Name on left of first comma (also tolerates en-dash separator).
RE_NAME_COMMA = re.compile(r'^([^,]+?),\s*[A-Za-z\d\u2018\u2019\u201C\u201D\u00C0-\u017F\'"]')
RE_NAME_DASH = re.compile(r'^([A-Z][\w.\'\-\s]+?)\s*[\u2014\u2013-]\s*[A-Z]')
# Strip leading death-marker prefix when the comma fell after the verb phrase
# rather than after the name. E.g. "Killed at 71, Ayman al-Zawahri Led a Life…"
RE_LEADING_DEATH_PREFIX = re.compile(
    r'^(?:Killed|Murdered|Slain|Assassinated|Dead|Dies?|Dying|Found\s+Dead)'
    r'\s+(?:at\s+\d{1,3}|in\s+\w+|on\s+\w+|by\s+\w+),\s*',
    re.I,
)
# "Eben Pyne 89, Who…" — name then space-separated age (no comma between).
# Insert the missing comma so the standard RE_NAME_COMMA picks up the name.
RE_NAME_AGE_NO_COMMA = re.compile(
    r'^([A-Z][\w.\'\-\u00C0-\u017F]+(?:\s+(?:[A-Z][\w.\'\-\u00C0-\u017F]+|[a-z]{1,3}))*)\s+(\d{2,3}),\s'
)
# Tagline-then-name: "A Man of Many Words, David Shulman Dies at 91". The
# pre-comma phrase is descriptive; the actual name is between the comma and
# the death verb. Captured group 1 = the name. Headline must start with a
# tagline-style word ("A", "An", "The") or contain a tell-tale function word.
RE_TAGLINE_NAME = re.compile(
    r'^(?:A|An|The)\s+[^,]+?,\s*'                                        # "A Man of Many Words, …"
    r'([A-Z][\w.\'\-\u00C0-\u017F]+(?:\s+(?:[A-Z][\w.\'\-\u00C0-\u017F]+|[a-z]{2,3}-?[A-Z]?[\w.\'\-\u00C0-\u017F]*))*?)'
    r'[,\s]+(?:Dies?|Is\s+Dead|Was\s+Dead|Died|Has\s+Died|Led|Was)\b',
)
# Generic tagline opener: a phrase with a function word before the name. Requires
# a comma separator before the death verb so we don't grab group-of-name
# constructions ("Mary Travers of Peter, Paul and Mary Dies at 72").
RE_TAGLINE_NAME_GENERIC = re.compile(
    r'^[A-Z][^,]*?\s(?:of|for|with|in|on|at|to|from|by)\s[^,]*?,\s*'
    r'([A-Z][\w.\'\-\u00C0-\u017F]+(?:\s+(?:[A-Z][\w.\'\-\u00C0-\u017F]+|[a-z]{2,3}-?[A-Z]?[\w.\'\-\u00C0-\u017F]*))*?)'
    r',\s*(?:Dies?|Is\s+Dead|Was\s+Dead|Died|Has\s+Died|Led|Was)\b',
)
# Function words that disqualify a comma-name candidate from being a real name
# (taglines like "A Man of Many Words" contain them; real names don't).
RE_HAS_FUNC_WORD = re.compile(r'\s(?:of|for|with|in|on|by|from|to|at|the|that)\s', re.I)

# Common nouns/verbs that betray a descriptive phrase rather than a person's name.
# A real name almost never contains these tokens; if extract_name returns a
# candidate containing one, it's almost certainly a tagline-as-name artifact.
DESCRIPTIVE_TOKENS = {
    'force', 'career', 'life', 'lives', 'fame', 'reign', 'novelist',
    'leader', 'wrestler', 'patron', 'founder', 'pioneer', 'champion',
    'legend', 'hero', 'icon', 'death',
    # Profession/role/relationship words that appear as the leading token in
    # essay-style headlines ("Outsider Whose Dark Vision…", "Wife of Billy
    # Graham…", "Owner of Segway Company…"). Safe to add since they're never
    # plausible given names or surnames.
    'outsider', 'scientist', 'activist', 'choreographer', 'architect',
    'astronaut', 'broadcaster', 'comedian', 'correspondent', 'diplomat',
    'economist', 'educator', 'entertainer', 'executive', 'filmmaker',
    'journalist', 'lawmaker', 'legislator', 'mathematician', 'musician',
    'naturalist', 'philanthropist', 'photographer', 'playwright',
    'politician', 'professor', 'reformer', 'sociologist', 'strategist',
    'theologian', 'urbanist', 'violinist',
    'wife', 'husband', 'widow', 'widower', 'owner', 'creator', 'inventor',
    'maker', 'builder', 'commander', 'pioneer', 'champion',
    # NOTE: 'art', 'star', 'song', 'novel' are too easily real first names
    # or surnames — including them blocks legitimate parses.
    # NOTE: 'hall' and 'young' removed — both are common surnames.
    'made', 'helped', 'wrote', 'founded', 'led', 'died', 'killed',
    'who', 'whose', 'whom', 'what', 'where', 'when', 'why',
    'former', 'late', 'old', 'famous', 'notable',
}
def looks_descriptive(name):
    if not name: return False
    toks = name.lower().split()
    return any(t.strip(".,;:") in DESCRIPTIVE_TOKENS for t in toks)
# Headline contains a death verb anywhere — pull the leading 1-4 capitalized
# tokens as the name. Handles:
#   - "Laurence Mancuso Dies; Founding Abbot Was 72" (semicolon, no age-after)
#   - "Whitey Bulger Is Dead in Prison at 89" (intervening "in Prison")
#   - "Joe Moakley of Massachusetts Dies at 74" (intervening "of …" clause)
#   - "Derek Freeman Dies at 84"
# Token whitelist excludes verb-form words that are also capitalized in
# headlines (Is, Was, Dies, Dead, Has, Had).
RE_HAS_DIES = re.compile(r'\b(?:Dies?|Is\s+Dead|Is\s+Dying|Was\s+Dead)\b', re.I)
_NOT_VERB = (r'(?!(?:Is|Was|Has|Had|Will|Are|Were|Dies?|Dead|Died|Dying|From|The|'
             r'Led|Made|Built|Founded|Wrote|Helped|Played|Knew|Spoke|Came|Went|'
             r'Of|For|With|And|But|Or|At|On|In|To|Who|That|Whose|Which)\b)')
# A "name token" is either:
#   - capitalized + word chars (David, O'Brien, Cartier-Bresson, Schlöndorff)
#   - a lowercase particle followed by hyphen + capital (al-Zawahri, bin-Laden)
#   - lowercase particles inside a name (von, de, van, der, du, della) — only
#     accepted as continuation tokens, not as the leading token.
_NAME_TOKEN = (
    r'(?:[A-Z][\w.\'\-\u00C0-\u017F]*'
    r'|(?:al|bin|el|abu)-[A-Z][\w.\'\-\u00C0-\u017F]*)'
)
_PARTICLE = r'(?:von|van|de|der|den|du|della|delle|di|da|do|dos|el|la|le|al|bin|abu|ten|ter)'
RE_LEADING_CAPS = re.compile(
    r'^(' + _NOT_VERB + _NAME_TOKEN +
    r'(?:\s+(?:' + _NOT_VERB + _NAME_TOKEN + r'|' + _PARTICLE + r')){0,4})'
)

RE_AGE_DIES = re.compile(
    # death-verb …optional short intervening phrase… "at NN"
    r'\b(?:Die[ds]?|Is\s+Dead|Was\s+Dead|Dead|Killed|Slain|Murdered)\b'
    r'(?:[^,;.\d]{1,40}?)?\s+at\s+(?:Age\s+|the\s+age\s+of\s+)?(\d{2,3})\b',
    re.I,
)
# `[,\b]` was a bug — \b is BACKSPACE inside a character class, not a word
# boundary. Replace with explicit terminators (comma, semicolon, whitespace,
# end of string) so "Fenelon, 74; Memoirs Described…" matches.
RE_AGE_COMMA_HEAD = re.compile(r',\s*(\d{2,3})\s*(?:[,;\s]|$)')
RE_AGE_WAS = re.compile(r'\bwas\s+(\d{2,3})\b', re.I)

# Common first names for gender fallback. Top-50 each, US 1940-2010 SSA.
MALE_NAMES = set("""
James John Robert Michael William David Richard Joseph Thomas Charles
Christopher Daniel Matthew Anthony Donald Mark Paul Steven Andrew Kenneth
George Joshua Kevin Brian Edward Ronald Timothy Jason Jeffrey Ryan Gary
Nicholas Eric Stephen Jonathan Larry Justin Scott Frank Brandon Benjamin
Gregory Samuel Raymond Patrick Alexander Jack Dennis Jerry Tyler Aaron
Henry Douglas Peter Jose Adam Nathan Zachary Walter Kyle Harold Carl Jeremy
Keith Roger Gerald Ethan Arthur Terry Christian Sean Lawrence Austin Joe
Noah Jesse Albert Bryan Billy Bruce Willie Jordan Dylan Alan Ralph Gabriel
Roy Juan Wayne Eugene Logan Randy Louis Russell Vincent Philip Bobby Johnny
Bradley Curtis Kenneth Howard Fred Stuart Lewis Theodore Ricky Earl Phillip
Phil Marvin Dale Cecil Floyd Clyde Norman Allen Ernest Stanley Leonard
Lloyd Bernard Glenn Eddie Calvin Travis Marcus Jeremiah Don Lance Cody
Mickey Maurice Wesley Stan Sidney Sam Dean Glen Marshall Dick Pat Toby
Edwin Tom Dwight Otis Bert Aldo Jorge Manuel Pedro Federico Carlos Luis
Jaime Jesus Roberto Mario Alfonso Anibal Alejandro Lazaro
""".split())

FEMALE_NAMES = set("""
Mary Patricia Linda Barbara Elizabeth Jennifer Maria Susan Margaret Dorothy
Lisa Nancy Karen Betty Helen Sandra Donna Carol Ruth Sharon Michelle Laura
Sarah Kimberly Deborah Jessica Shirley Cynthia Angela Melissa Brenda Amy
Anna Rebecca Virginia Kathleen Pamela Martha Debra Amanda Stephanie Carolyn
Christine Marie Janet Catherine Frances Ann Joyce Diane Alice Julie Heather
Teresa Doris Gloria Evelyn Jean Cheryl Mildred Katherine Joan Ashley Judith
Rose Janice Kelly Nicole Judy Christina Kathy Theresa Beverly Denise Tammy
Irene Jane Lori Rachel Marilyn Andrea Kathryn Louise Sara Anne Jacqueline
Wanda Bonnie Julia Ruby Lois Tina Phyllis Norma Paula Diana Annie Lillian
Emily Robin Peggy Crystal Gladys Rita Dawn Connie Florence Tracy Edna
Tiffany Carmen Rosa Cindy Grace Wendy Victoria Edith Kim Sherry Sylvia
Josephine Thelma Shannon Sheila Ethel Ellen Elaine Marjorie Carrie Charlotte
Monica Esther Pauline Emma Juanita Anita Rhonda Hazel Amber Eva Debbie April
Leslie Clara Lucille Jamie Joanne Eleanor Valerie Danielle Megan Alicia Suzanne
Michele Gail Bertha Darlene Veronica Jill Erin Geraldine Lauren Cathy Joann
Lorraine Lynn Sally Regina Erica Beatrice Dolores Bernice Audrey Yvonne
Annette June Marion Dana Stacy Ana Renee Ida Vivian Roberta Holly Brittany
Melanie Loretta Yolanda Jeanette Laurie Katie Kristen Vanessa Alma Sue Elsie
Beth Jeanne Vicki Carla Tara Rosemary Eileen Terri Gertrude Lucy Tonya Ella
Stacey Wilma Gina Kristin Jessie Natalie Agnes Vera Willie Charlene Bessie
Delores Melinda Pearl Arlene Maureen Colleen Allison Tamara Joy Georgia
Constance Lillie Claudia Jackie Marcia Tanya Nellie Minnie Marlene Heidi
Glenda Lydia Viola Courtney Marian Stella Caroline Dora Jo Vickie Mattie
Nina Ophelia Pippa
""".split())


def clean_smart_quotes(s):
    """Drop curly/straight quotes that confuse the parser."""
    if not s: return s
    return re.sub(r'[\u2018\u2019\u201C\u201D\'"]+', '', s).strip()


# Honorifics / titles to peel off the start of the name.
# Long-form first (so "The Reverend" matches before "Rev"); each entry includes
# trailing dot variants. Strips these before the name itself.
TITLE_PREFIXES = (
    r'(?:Former|Ex-|Ex|Late|State|Vice|Brig\.?|Episcopal|Roman\s+Catholic|Catholic|Anglican|Methodist|Baptist)?\s*'
    r'(?:The\s+)?(?:Reverend|Rev\.?|Rev|Father|Fr\.?|Pastor|Bishop|Cardinal|'
    r'Sister|Brother|Mother|Rabbi|Imam|Sheikh|Sheik|Sri|Mahatma|'
    r'Sir|Dame|Lord|Lady|Baron|Baroness|Count|Countess|Duke|Duchess|'
    r'Prince|Princess|King|Queen|Emperor|Empress|'
    r'Dr\.?|Doctor|Professor|Prof\.?|Justice|Judge|Senator|Sen\.?|'
    r'Representative|Rep\.?|Governor|Gov\.?|Mayor|President|Chief|'
    r'General|Gen\.?|Colonel|Col\.?|Major|Maj\.?|Captain|Capt\.?|'
    r'Lieutenant|Lt\.?|Admiral|Adm\.?|Commander|Cmdr\.?|Sergeant|Sgt\.?|'
    r'Mr\.?|Mrs\.?|Ms\.?|Mx\.?|Miss|'
    r'Madame|Madam|Monsieur|Mademoiselle|'
    r'Se(?:n|ñ)or|Se(?:n|ñ)ora|Se(?:n|ñ)orita|'
    r'Sultan|Sultana|Emir|Sheikh|Sheik|'
    r'Saint|St\.?)'
)
RE_LEADING_TITLE = re.compile(r'^' + TITLE_PREFIXES + r'\s+', re.I)
# Same as RE_LEADING_TITLE but with a capture group so we can recover the
# title prefix for display_name (e.g. "Sister Andre" instead of "Andre").
RE_LEADING_TITLE_CAP = re.compile(r'^(' + TITLE_PREFIXES + r')\s+', re.I)


def make_display_name(headline, name):
    """Return name with any leading honorific/title from the headline preserved.
    Sister André → 'Sister André'; Sir Patrick Stewart → 'Sir Patrick Stewart'.
    Suffixes (Jr., 2nd, 3rd) are already inside `name` and don't need recovery.
    """
    if not name or not headline:
        return name
    h = headline.replace('\u200b', '').replace('\ufeff', '').strip()
    h = RE_FROM_YEAR.sub('', h)
    h = RE_LEADING_SERIES.sub('', h)
    h = RE_LEADING_DEATH_PREFIX.sub('', h)
    h = re.sub(r'\s*\([^)]*\)', '', h)
    titles = []
    # Apply iteratively to catch stacked titles ("Sir Dr. ...").
    for _ in range(3):
        m = RE_LEADING_TITLE_CAP.match(h)
        if not m:
            break
        titles.append(re.sub(r'\s+', ' ', m.group(1).strip()))
        h = h[m.end():]
    if not titles:
        return name
    title_str = ' '.join(titles)
    # Don't double-prefix if the parsed name already starts with the title.
    if name.lower().startswith(title_str.lower()):
        return name
    return f'{title_str} {name}'
# "What They Left Behind:" is a recurring NYT Magazine end-of-year series
RE_LEADING_SERIES = re.compile(
    r'^(?:Overlooked No More|What They Left Behind|The Lives They Lived|Lives They Lived|'
    r'A Life Lived|Living On|In Memoriam|Not Forgotten)\s*[:\u2014\u2013-]\s*',
    re.I,
)
# "From 1992: …" — Times republishes old obits as packages (e.g., Women's
# History Month). Distinct from Overlooked No More (which is *new* coverage of
# someone never covered). Strip the prefix so the original headline can parse.
# Also tolerates a stacked "From From 1992:" / "From: From 1992:" the desk
# sometimes lets through.
RE_FROM_YEAR = re.compile(
    r'^(?:From(?:\s*[:\u2014\u2013-]\s*|\s+))?From\s+\d{4}\s*[:\u2014\u2013-]\s*|'
    r'^From\s+\d{4}\s*[:\u2014\u2013-]\s*',
    re.I,
)
# Republished-obit boilerplate that prepends the lead paragraph. We strip it
# before pronoun/honorific scanning so gender detection sees the actual prose.
RE_REPUB_BOILER = re.compile(
    r'This\s+obituary\s+was\s+originally\s+published\s+on[^.]+\.[^.]*?'
    r'(?:republished|reissued)[^.]*?\.\s*',
    re.I,
)
# Year-end / memoriam roundup slugs that aren't single-subject obituaries.
# Slideshows and videos are also dropped — many sit on the Obits desk but
# carry no parseable name in the headline (e.g. "A Look at Hiro's Work").
RE_NON_OBIT_URL = re.compile(
    r'(?:'
    r'obituaries-deaths-\d{4}|'         # year-end roundups: /obituaries-deaths-2023
    r'/learning/lesson-plans/|'          # NYT Learning lesson plans
    r'/slideshow/|'                      # photo packages
    r'/video/|'                          # video obits
    r'\.mp3(?:\?|$)|'                    # audio obits
    r'/podcasts\.nytimes\.com/|'         # podcast hosts (joygolden mp3 etc.)
    r'in-a-political-year-some-deaths|'  # 2024 Navalny package
    r'lives-they-lived|'                 # NYT Magazine year-end issue
    r'article-\d+-no-title|'             # API placeholder records ("Article 2002… No Title")
    r'us-repeats-north-korea-stance|'    # 2003 misclassified national-section article
    r'among-deaths-in-\d{4}|'            # year-end "Among Deaths in 2016, a Heavy Toll…"
    r'in-a-year-of-notable-deaths|'      # year-end "In a Year of Notable Deaths…"
    r'notable-deaths(?:-|\.html)|'        # year-end Notable Deaths interactives & 2024 promo
    r'notable-women-deaths|'              # 2026 "100 Years of Women Who Changed History"
    r'people-died-coronavirus|'          # 2020 "Those We've Lost" interactive
    r'martin-luther-king-day-black-leaders|'  # 2016 MLK Day feature, not single obit
    r'/document-[A-Za-z]+-Speech|'       # interactive documents (Goodwin speech)
    r'/\d{4}-(?:covid-)?deaths\.html|'   # year-end "2020-deaths.html", "2020-covid-deaths.html"
    r'/deaths-in-\d{4}\.html|'           # year-end "deaths-in-2018.html"
    r'/mothers-day(?:[-./]|$)|'          # Mother's Day interactives
    r'/obituaries/archives(?:/|$)|'      # interactive root pages (Not Forgotten Jesse Owens)
    r'/2018/obituaries/overlooked\.html|'  # Overlooked root index page
    r'/projects/cp/obituaries/archives/|' # archive-feature pages (moon-neil-armstrong-nasa, july4-…)
    r'\d{4}-deaths-obituaries|'           # year-end roundup ("Gone in 2025: A Yearlong Procession…")
    r'/coronavirus-victims\b|'            # reader-input form, not an obit
    r'/musicians-who-died|'               # year-end musician roundup
    r'/what-they-left-behind|'            # multi-subject NYT Magazine feature
    r'/dead-what-they-left-behind|'       # variant slug for the same feature
    r'/\d+-women-who-changed|'            # group features ("5 Women Who Changed…")
    r'/black-history-month-overlooked|'   # Overlooked index for Black History Month
    r'/overlooked-obituary-grandmothers|' # Reader Center grandmothers feature
    r'/overlooked-from-the-death-desk|'   # Death Desk meta article
    r'/formacist-overlooked|'             # "Have an Idea?" reader-suggestion form
    r'/overlooked-nominations'            # 2023 reader-suggestion form
    r')',
    re.I,
)
# Specific URL blocklist — paid memorial notices and one-off republications that
# look like obits but aren't editorial obits. Indexed under tom=Obituary.
# Store path-only URLs (matching how build normalizes web_url before checking).
NON_OBIT_URLS = {
    '/2006/09/16/obituaries/16feuer.html',           # Cynthia Feuer memorial
    '/2006/09/17/obituaries/17stapleton.html',       # Maureen Stapleton memorial
    '/2007/02/09/us/09smith.html',                   # Anna Nicole Smith news / Robert Altman memorial
    '/2007/03/02/obituaries/02ertegun.html',         # Ahmet Ertegun memorial
    '/2007/04/16/obituaries/16schlesinger.html',     # Arthur Schlesinger memorial
    '/2007/02/05/obituaries/05ivins.html',           # Molly Ivins memorial
    '/2026/03/06/us/politics/eleanor-roosevelt-dead.html',  # republication ("Mrs. Roosevelt")
    # 2026-04-25 manual review (xlsx) — interactive list packages, not obits
    '/2016/12/12/obituaries/most-read-obituaries.html',
    '/2019/07/05/obituaries/apollo-11-moon-obituaries.html',
    '/interactive/2016/01/13/obituaries/breaking-bread-petraeus.html',
    '/interactive/2016/01/13/obituaries/newton-obits.html',
    '/interactive/2016/01/13/obituaries/russert-obits.html',
    '/interactive/2016/01/13/obituaries/stonewall-pine-delarverie-obits.html',
    '/interactive/2016/01/13/obituaries/summer-obits.html',
    '/interactive/2016/01/13/obituaries/zaharias-obits.html',
    '/interactive/2016/06/30/obituaries/july4-copy.html',
    '/interactive/2016/06/30/obituaries/moon-landing.html',
    '/interactive/2016/07/22/obituaries/cassius-clay.html',
    '/interactive/2016/07/22/obituaries/nf-farewell.html',
    '/interactive/2016/08/14/obituaries/india-hp.html',
    '/interactive/2021/03/25/obituaries/womens-history-month-obituaries.html',
    '/interactive/2016/08/25/obituaries/capote-obits.html',  # group of obits ("In Cold Blood…")
    # 2026-04-25 /interactive/ audit — non-obit features
    '/interactive/2011/11/06/obituaries/rooney-video-gallery.html',          # Andy Rooney video gallery
    '/interactive/2016/01/13/obituaries/anderson-cooper-obits.html',         # Cooper alive, on his father
    '/interactive/2016/01/13/obituaries/tom-brokaw-obits.html',              # Breaking Bread feature
    '/interactive/2016/07/22/obituaries/dawes-breakingbread.html',           # Breaking Bread feature
    '/interactive/2017/02/17/obituaries/17stambler-encyclopedia-excerpts.html',  # book excerpts

    # ---- 2026-04-27 manual review (Corrected obits.xlsx) ----
    '/2019/06/05/obituaries/tonys-nominees-obits.html',
    # Letter-of-remembrances feature, not an obit
    '/2006/10/04/obituaries/apple-remember.html',
    # Personal essay/appreciation, not an obit (Stan Lee)
    '/2018/11/12/obituaries/my-moments-with-stan.html',
    # Tribute essay, not an obit (Robert Heilbroner)
    '/2005/01/28/obituaries/a-tribute-for-robert-heilbroner.html',
    # Memorial-service announcements (not obits)
    '/2007/02/19/obituaries/19altman.html',          # Robert Altman memorial service
    '/2006/09/16/obituaries/memorial-for-cy-feuer.html',  # Cy Feuer memorial
    # ---- 2026-04-27 unparsed.xlsx — non-obit features ----
    '/2025/10/28/dining/obituary-cocktail-new-orleans.html',                   # cocktail recipe article
    '/2005/04/02/world/europe/obituary-a-man-of-action.html',                  # JP II sidebar essay
    '/2020/05/29/podcasts/the-daily/obituaries-coronavirus-100000.html',       # Daily podcast
    '/article/obituary-suggestions.html',                                      # reader-suggestion solicitation
    '/interactive/2016/06/30/obituaries/shootings-part3.html',                 # interactive feature
    # Reader Center "Overlooked" reader-submission feature, not an obit
    '/2018/05/02/reader-center/obituary-suggestions-overlooked.html',
    # Maureen Stapleton memorial-service announcement (sibling of /2006/09/16/obituaries/17stapleton.html)
    '/2006/09/17/obituaries/stapleton-memorial.html',
}

# Per-URL corrections for records the parsers can't recover programmatically:
# - name: headline says "Brazilian Nun" instead of the subject's actual name
# - gender: pronoun count is misleading (often when the subject's name appears
#   in a way that triggers the wrong baby-name match, or the obit quotes a
#   different-gender colleague heavily)
# - age: republished obits whose source year predates 2000 (no raw-dump record
#   to cross-reference) and whose abstract carries no age phrase
OBIT_OVERRIDES = {
    # Sister Inah Canabarro Lucas: store name without "Sister" — title is
    # added back via make_display_name from the headline. Keeps internal
    # name consistent with Dr/Rev/Sir stripping policy.
    '/2025/05/02/world/americas/inah-canabarro-lucas-oldest-person-dead.html': {
        'name': 'Inah Canabarro Lucas', 'profession': None,
    },
    # Leslie Edwards (dancer, Royal Ballet) — male; pronoun count in our extracts
    # came out F (likely because the abstract referenced a female partner).
    '/2001/02/12/arts/leslie-edwards-84-dancer-with-a-mime-s-touch.html': {
        'gender': 'M', 'gender_src': 'manual',
    },
    # Pre-2000 republications: cross-year lookup can't reach the original.
    '/2026/02/17/world/europe/anna-akhmatova-dead.html':       {'age': 76},
    '/2026/02/17/world/europe/fania-fenelon-dead.html':        {'age': 74},
    '/2026/03/06/world/asia/jiang-qing-dead.html':             {'age': 77},
    '/2026/03/06/world/americas/gabriela-mistral-dead.html':   {'age': 67},
    # Other pre-2000 republications whose headlines lack a parseable age.
    '/2026/03/06/business/media/nellie-bly-dead.html':         {'age': 57},
    '/2026/03/06/obituaries/jane-addams-dead.html':            {'age': 74},
    '/2026/03/06/world/asia/indira-gandhi-dead.html':          {'age': 67},
    '/2026/03/06/movies/natalie-wood-dead.html':               {'age': 43},
    '/2026/03/06/movies/marilyn-monroe-dead.html':             {'age': 36},
    '/2026/03/06/sports/babe-zaharias-dead.html':              {'age': 45},
    # Selena: headline is "Grammy-Winning Singer Selena Killed…" — parser
    # captures "Grammy" as a name token. Real name + age from the obit body.
    '/2026/03/06/arts/music/selena-dead.html': {
        'name': 'Selena Quintanilla Pérez', 'age': 23,
    },
    # Queen Elizabeth the Queen Mother: headline says "Britain's Beloved 'Queen
    # Mum,' A Symbol of Courage, Dies at 101" — birth name is Elizabeth Bowes-
    # Lyon. Use the title-form name commonly used in her obituary tagline.
    '/2002/03/31/world/britain-s-beloved-queen-mum-a-symbol-of-courage-dies-at-101.html': {
        'name': 'Queen Elizabeth the Queen Mother', 'profession': None,
    },
    # Aaron Swartz: headline starts "Internet Activist, a Creator of RSS, Is
    # Dead at 26" — the name is in the URL slug only.
    '/2013/01/13/technology/aaron-swartz-internet-activist-dies-at-26.html': {
        'name': 'Aaron Swartz', 'profession': 'Internet Activist',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Akebono Taro: headline reads "Akebono, First Foreign-Born Sumo Grand
    # Champion, Dies at 54" — parser captures only the ring name; slug carries
    # the full Japanese name "akebono-taro".
    '/2024/04/10/world/asia/akebono-taro-sumo-dead.html': {
        'name': 'Akebono Taro',
    },
    # King Abdullah: profession parses as "Shrewd Force" from the headline
    # tagline. He was the King of Saudi Arabia.
    '/2015/01/23/world/middleeast/king-abdullah-who-nudged-saudi-arabia-forward-dies-at-90.html': {
        'profession': 'King of Saudi Arabia',
    },
    # Cardinal John O'Connor: 2000 headline is all-caps ("CARDINAL O'CONNOR…"),
    # so name parses as just "O'CONNOR" and display_name is "CARDINAL O'CONNOR".
    # Abstract uses the proper "Cardinal John O'Connor".
    '/2000/05/04/nyregion/death-of-a-cardinal-cardinal-o-connor-80-dies-forceful-voice-for-vatican.html': {
        'name': "Cardinal John O'Connor", 'profession': 'Archbishop of New York',
        'gender': 'M', 'gender_src': 'manual',
    },
    # ---- 2026-04-25 manual review (xlsx) — gender + name/role/age corrections ----
    '/2000/05/15/world/keizo-obuchi-premier-who-brought-stability-japan-s-economy-faltered-dies-62.html': {'gender': 'M', 'gender_src': 'manual', 'profession': 'Japanese Prime Minister'},
    '/2001/04/23/arts/giuseppe-sinopoli-intense-physical-conductor-dies-54-after-collapsing-onstage.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2002/04/29/world/aleksandr-lebed-52-dies-midwife-of-russian-democracy.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2002/11/18/theater/crash-kills-william-marrie-33-a-lead-dancer-in-movin-out.html': {'gender': 'M', 'gender_src': 'manual', 'name': 'William Marrié'},
    '/2003/02/13/nyregion/neville-colman-pathologist-and-dna-expert-dies-at-57.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2003/08/12/sports/herb-brooks-66-dies-in-auto-accident-coached-us-olympians-to-miracle-on-ice.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2004/01/15/obituaries/olivia-goldsmith-who-wrote-comic-first-wives-club-dies-at-54.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2004/02/29/nyregion/lady-fiennes-muse-of-a-british-explorer-is-dead-at-56.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2004/03/20/business/sherman-lewis-67-financier-who-was-executive-at-lehman.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2004/05/03/us/aj-naparstek-65-public-housing-expert.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2004/08/17/nyregion/kermit-s-champa-64-author-and-distinguished-art-historian.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2004/12/01/arts/design/ed-paschke-painter-65-dies-pop-artist-with-dark-vision.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2004/12/05/nyregion/obituaries/lucien-hold-early-champion-of-top-comics-is-dead-at-57.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2004/12/12/sports/obituaries/f-darrin-perry-39-dies-designed-espn-magazine.html': {'gender': 'M', 'gender_src': 'manual', 'profession': "Lead Dancer in 'Movin' Out'"},
    '/2005/01/14/business/media/jay-schulberg-dies-at-65-creator-of-milk-campaign.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2005/10/03/theater/newsandfeatures/august-wilson-theaters-poet-of-black-america-is.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2006/05/23/world/23lee.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2006/07/04/world/americas/04lewites.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2006/08/24/business/24do.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2006/10/01/education/30wakeman.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2008/09/19/world/europe/19dagueneau.html': {'gender': 'M', 'gender_src': 'manual', 'name': 'Didier Dagueneau'},
    '/2013/03/24/world/europe/boris-a-berezovsky-a-putin-critic-dies-at-67.html': {'gender': 'M', 'gender_src': 'manual', 'name': 'Boris A. Berezovsky', 'profession': 'Russian Oligarch'},
    '/2016/07/29/obituaries/matilda-rapaport-extreme-skier.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2020/04/23/obituaries/ketty-herawati-sultana-coronavirus-dead.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2020/04/28/health/mel-baggs-dead.html': {'gender': 'X', 'gender_src': 'manual'},
    '/2020/11/24/obituaries/honestie-hodges-dead-coronavirus.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2020/12/18/us/benny-napoleon-dead.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2021/05/01/obituaries/manisha-jadhav-dead-coronavirus.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2024/12/04/style/rohit-bal-dead.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2025/03/07/arts/music/dwayne-wiggins-dead.html': {'gender': 'M', 'gender_src': 'manual', 'name': "D'Wayne Wiggins"},
    '/2025/11/24/science/gramma-galapagos-tortoise-san-diego-zoo-dies.html': {'gender': 'F', 'gender_src': 'manual', 'age': 141},
    # Non-binary obits surfaced via Google site:nytimes.com "Mx." search
    '/2024/11/18/theater/morgan-jenness-dead.html': {'gender': 'X', 'gender_src': 'manual'},
    '/2025/06/27/theater/diana-oh-dead.html': {'gender': 'X', 'gender_src': 'manual'},
    # ---- 2026-04-25 /interactive/ audit — name/profession swaps & parsing breakage ----
    # Bruce Lee: headline "Not Forgotten: A Fighter's Fighter, Bruce Lee" — parser
    # took the descriptor as name and the real name as profession. Swap.
    '/interactive/2016/06/30/obituaries/bruce-lee.html': {
        'name': 'Bruce Lee', 'profession': "A Fighter's Fighter",
        'gender': 'M', 'gender_src': 'manual',
    },
    # Ella Fitzgerald: parser took "Fitzgerald Obits" from URL slug.
    '/interactive/2016/01/13/obituaries/fitzgerald-obits.html': {
        'name': 'Ella Fitzgerald', 'gender': 'F', 'gender_src': 'manual',
    },
    # Ruhollah Khomeini: profession parsed as "Man"; name had "Iran's" prefix.
    '/interactive/2016/01/13/obituaries/summer-obits-copy.html': {
        'name': 'Ruhollah Khomeini', 'profession': 'A Man Who Shook the World',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Robert F. Kennedy: headline "On One California Night, Triumph and Tragedy"
    # carries no name; subject inferred from context (RFK assassination, June 1968).
    '/interactive/2016/01/13/obituaries/summer-obits-kennedy.html': {
        'name': 'Robert F. Kennedy', 'profession': 'Triumph and Tragedy',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Billy the Kid: parser dropped "the" between Billy and Kid.
    '/interactive/2016/06/30/obituaries/billy-kid.html': {
        'name': 'Billy the Kid', 'profession': 'An Outlaw by Any Name',
        'gender': 'M', 'gender_src': 'manual',
    },
    # John F. Kennedy Jr.: URL slug "jfkjr-copy" → "Jfkjr Copy".
    '/interactive/2016/06/30/obituaries/jfkjr-copy.html': {
        'name': 'John F. Kennedy Jr.', 'profession': 'A Life Cut Short',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Laurence Olivier: headline "Laurence Olivier: Scene-Stealer Extraordinaire"
    # was split on the colon, putting "Scene" as name.
    '/interactive/2016/06/30/obituaries/nf-olivier.html': {
        'name': 'Laurence Olivier', 'profession': 'Scene-Stealer Extraordinaire',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Princess Diana: parser kept only "Diana"; profession truncated to "Was Beloved".
    '/interactive/2016/06/30/obituaries/princess-diana-obits.html': {
        'name': 'Princess Diana', 'profession': 'Beloved, Yet Troubled',
        'gender': 'F', 'gender_src': 'manual',
    },
    # Jesse Owens: name parsed as None.
    '/interactive/2016/07/22/obituaries/owens.html': {
        'name': 'Jesse Owens', 'gender': 'M', 'gender_src': 'manual',
    },
    # Hans Christian Andersen: URL slug "andersen-sf" → "Andersen Sf".
    '/interactive/2016/08/03/obituaries/andersen-sf.html': {
        'name': 'Hans Christian Andersen', 'profession': 'Sprung From Poverty',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Raymond Smullyan: parser took the headline's "Large Birds" (referring to
    # the puzzles in his book) as name.
    '/interactive/2017/02/11/obituaries/smullyan-logic-puzzles.html': {
        'name': 'Raymond Smullyan', 'profession': 'Logic Puzzles',
        'gender': 'M', 'gender_src': 'manual',
    },
    # Major Taylor: parser dropped "Major" (treated as honorific).
    '/interactive/2019/obituaries/major-taylor-overlooked.html': {
        'name': 'Major Taylor', 'gender': 'M', 'gender_src': 'manual',
    },
    # Mary Ellen Pleasant: headline "The Many Chapters of Mary Ellen Pleasant"
    # gave parser only "Mary Ellen".
    '/interactive/2019/obituaries/mary-ellen-pleasant-overlooked.html': {
        'name': 'Mary Ellen Pleasant', 'gender': 'F', 'gender_src': 'manual',
    },
    # Moses Fleetwood Walker: headline was just "Moses Fleetwood Walker" but
    # parser truncated to "Moses Fleetwood".
    '/interactive/2019/obituaries/moses-fleetwood-walker-overlooked.html': {
        'name': 'Moses Fleetwood Walker', 'gender': 'M', 'gender_src': 'manual',
    },
    # Zelda Wynn Valdes: headline "Zelda Wynn, Fashion Designer..." dropped Valdes.
    '/interactive/2019/obituaries/zelda-wynn-valdes-overlooked.html': {
        'name': 'Zelda Wynn Valdes', 'gender': 'F', 'gender_src': 'manual',
    },

    # ---- 2026-04-27 manual review (Corrected obits.xlsx, 130 rows) ----
    '/2000/02/08/business/gad-rausing-77-swedish-innovator-of-beverage-containers.html': {'gender': 'M', 'gender_src': 'manual', 'age': 77},
    '/2000/05/12/world/gervase-cowell-73-manager-of-a-soviet-turncoat-spy-dies.html': {'gender': 'M', 'gender_src': 'manual', 'age': 73},
    '/2000/06/22/nyregion/elvin-kabat-85-microbiologist-known-for-work-in-immunology.html': {'gender': 'M', 'gender_src': 'manual', 'age': 85},
    '/2000/08/26/nyregion/l-b-boyer-84-psychoanalyst-promoted-countertransference.html': {'gender': 'M', 'gender_src': 'manual', 'age': 84},
    '/2000/09/08/nyregion/t-l-deglin-92-public-relations-executive.html': {'age': 92},
    '/2000/10/13/world/hendrik-casimir-90-theorist-in-study-of-quantum-mechanics.html': {'age': 90},
    '/2000/12/11/world/vlado-gotovac-a-voice-of-freedom-in-croatia-dies-at-70.html': {'gender': 'M', 'gender_src': 'manual', 'age': 70},
    '/2001/03/04/world/c-m-woodhouse-writer-on-modern-greece-dies-at-83.html': {'gender': 'M', 'gender_src': 'manual', 'age': 83},
    '/2001/03/13/us/s-dillon-ripley-dies-87-led-smithsonian-institution-during-its-greatest-growth.html': {'age': 87},
    '/2001/03/24/us/rowland-evans-79-tv-host-and-conservative-columnist.html': {'gender': 'M', 'gender_src': 'manual', 'age': 79},
    '/2001/10/12/national/portraits/tawanna-griffin-family-meant-everything.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2001/12/02/national/portraits/jrme-r-lohez-vive-lamrique.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2001/12/02/national/portraits/weibin-wang-living-the-american-dream-2001120293520614313.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2001/12/08/national/portraits/gayle-greene-two-trips-a-year.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2001/12/15/national/portraits/dominick-pezzulo-the-unusual-was-typical.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2001/12/15/nyregion/a-keith-smiley-91-executive-of-resort-and-preservationist.html': {'age': 91},
    '/2002/07/14/world/yousuf-karsh-who-photographed-famous-and-infamous-of-20th-century-dies-at-93.html': {'age': 93},
    '/2002/08/08/business/cortlandt-parker-80-publisher-of-17-weeklies-in-new-jersey.html': {'gender': 'M', 'gender_src': 'manual', 'age': 80},
    '/2002/08/25/sports/hoyt-wilhelm-first-reliever-in-the-hall-of-fame-dies.html': {'gender': 'M', 'gender_src': 'manual', 'age': 79},
    '/2002/09/09/sports/frankie-albert-a-pioneering-quarterback-is-dead-at-82.html': {'gender': 'M', 'gender_src': 'manual', 'age': 82},
    '/2002/12/05/world/achille-castiglioni-84-modern-design-leader.html': {'gender': 'M', 'gender_src': 'manual', 'age': 84},
    '/2002/12/13/world/nani-palkhivala-82-dies-civil-rights-leader-in-india.html': {'gender': 'M', 'gender_src': 'manual', 'age': 82},
    '/2003/01/09/business/siggi-b-wilzig-76-executive-and-survivor-of-the-holocaust.html': {'gender': 'M', 'gender_src': 'manual', 'age': 76},
    '/2003/03/23/us/col-edson-raff-95-dies-led-paratroopers-in-1942.html': {'age': 95},
    '/2003/06/06/world/natalya-reshetovskaya-84-is-dead-solzhenitsyn-s-wife-questioned-gulag.html': {'gender': 'F', 'gender_src': 'manual', 'age': 84},
    '/2003/09/11/nyregion/m-konvitz-scholar-of-law-and-idealism-is-dead-at-95.html': {'age': 95},
    '/2003/09/24/world/simcha-dinitz-74-ex-israeli-envoy-had-role-in-disputed-airlift.html': {'gender': 'M', 'gender_src': 'manual', 'age': 74},
    '/2004/01/06/business/takashi-ishihara-91-dies-led-nissan-s-rise.html': {'age': 91},
    '/2004/01/15/theater/uta-hagen-tony-winning-broadway-star-and-teacher-of-actors-dies-at-84.html': {'gender': 'F', 'gender_src': 'manual', 'age': 84},
    '/2004/02/04/nyregion/adella-wotherspoon-last-survivor-of-general-slocum-disaster-is-dead-at-100.html': {'age': 100},
    '/2004/02/06/nyregion/trude-wenzel-lash-95-an-advocate-for-children.html': {'age': 95},
    '/2004/03/01/nyregion/labe-scheinberg-78-physician-and-multiple-sclerosis-specialist.html': {'gender': 'M', 'gender_src': 'manual', 'age': 78},
    '/2004/03/15/arts/vilayat-khan-76-musician-who-redefined-sitar-playing.html': {'gender': 'M', 'gender_src': 'manual', 'age': 76},
    '/2004/03/17/arts/genevieve-83-french-singer-who-mutilated-english-on-tv.html': {'gender': 'F', 'gender_src': 'manual', 'age': 83},
    '/2004/04/08/world/larisa-bogoraz-soviet-dissident-dies-at-74.html': {'gender': 'F', 'gender_src': 'manual', 'age': 74},
    '/2004/05/19/arts/elvin-jones-jazz-drummer-with-coltrane-dies-at-76.html': {'gender': 'M', 'gender_src': 'manual', 'age': 76},
    '/2004/06/07/nyregion/m-searle-wright-86-teacher-composer-and-organ-expert.html': {'age': 86},
    '/2004/06/15/nyregion/whitman-knapp-95-dies-exposed-police-corruption.html': {'age': 95},
    '/2004/07/07/sports/rodger-ward-83-two-time-indianapolis-500-winner.html': {'gender': 'M', 'gender_src': 'manual', 'age': 83},
    '/2004/07/07/us/tf-mancuso-who-led-radiation-study-dies-at-92.html': {'age': 92},
    '/2004/07/11/nyregion/corrine-grad-coleman-77-radical-feminist-and-writer.html': {'gender': 'F', 'gender_src': 'manual', 'age': 77},
    '/2004/07/12/business/laurance-s-rockefeller-passionate-conservationist-and-investor-is-dead-at-94.html': {'age': 94},
    '/2004/07/12/theater/phoebe-brand-96-actress-and-group-theater-co-founder.html': {'age': 96},
    '/2004/09/22/us/w-c-reeves-crucial-ally-in-west-nile-fight-dies-at-87.html': {'age': 87},
    '/2004/09/26/obituaries/w-dorwin-teague-94-industrial-designer-is-dead.html': {'age': 94},
    '/2004/09/30/books/mulk-raj-anand-99-famed-indian-writer-dies.html': {'age': 99},
    '/2004/10/10/politics/townsend-hoopes-82-author-who-wrote-about-vietnam-dies.html': {'gender': 'M', 'gender_src': 'manual', 'age': 82},
    '/2004/11/26/us/langdon-gilkey-85-theorist-on-nexus-of-faith-and-science-dies.html': {'gender': 'M', 'gender_src': 'manual', 'age': 85},
    '/2005/01/06/obituaries/maclyn-mccarty-dies-at-93-pioneer-in-dna-research.html': {'age': 93},
    '/2005/01/10/obituaries/r-bruce-mcgill-84-educator-who-led-development-of-tests.html': {'gender': 'M', 'gender_src': 'manual', 'age': 84},
    '/2005/01/17/obituaries/suzie-frankfurt-73-a-decorator-and-friend-to-warhol-dies.html': {'gender': 'F', 'gender_src': 'manual', 'age': 73},
    '/2005/01/20/science/h-bentley-glass-provocative-science-theorist-dies-at-98.html': {'age': 98},
    '/2005/03/10/theater/trude-rittmann-an-arranger-of-broadway-favorites-dies-at-96.html': {'age': 96},
    '/2005/03/23/arts/design/kenzo-tange-architect-of-urban-japan-dies-at-91.html': {'age': 91},
    '/2005/03/24/arts/design/czeslaw-slania-83-engraver-of-postage-stamps-and-money-dies.html': {'gender': 'M', 'gender_src': 'manual', 'age': 83},
    '/2005/04/15/college/andr-franois-is-dead-at-89-illustrator-with-biting-satire.html': {'name': 'AndrÃ© FranÃ§ois', 'age': 89},
    '/2005/04/30/sports/hockey/red-horner-one-of-hockeys-toughest-players-dies-at-95.html': {'age': 95},
    '/2005/07/21/business/gerry-thomas-who-thought-up-the-tv-dinner-is-dead-at-83.html': {'gender': 'M', 'gender_src': 'manual', 'age': 83},
    '/2005/07/23/arts/music/blue-barron-91-bigband-leader-dies.html': {'age': 91},
    '/2005/11/04/nyregion/waldemar-nielsen-expert-on-philanthropy-dies-at-88.html': {'age': 88},
    '/2006/01/31/arts/design/nam-june-paik-73-dies-pioneer-of-video-art-whose-work-broke.html': {'gender': 'M', 'gender_src': 'manual', 'age': 73},
    '/2006/04/02/nyregion/matt-kennedy-101-dies-stalwart-of-coney-island.html': {'age': 101},
    '/2006/04/12/style/bobbie-nudie-purveyor-of-glitter-to-rhinestone-cowboys-dies-at-92.html': {'age': 92},
    '/2006/04/13/movies/rajkumar-beloved-indian-film-star-dies-at-77.html': {'gender': 'M', 'gender_src': 'manual', 'age': 77},
    '/2006/04/15/nyregion/dr-paulina-f-kernberg-child-psychiatrist-dies-at-71.html': {'gender': 'F', 'gender_src': 'manual', 'age': 71},
    '/2006/05/02/world/europe/02revel.html': {'gender': 'M', 'gender_src': 'manual', 'name': 'Jean-François Revel', 'age': 82},
    '/2006/06/01/arts/01aarons.html': {'age': 89},
    '/2006/06/10/arts/10sano.html': {'gender': 'M', 'gender_src': 'manual'},
    '/2006/07/03/us/03bullough.html': {'gender': 'M', 'gender_src': 'manual', 'age': 77},
    '/2006/07/04/arts/television/04murray.html': {'age': 89},
    '/2006/07/12/arts/12hughes.html': {'age': 90},
    '/2006/07/27/us/27mosteller.html': {'age': 89},
    '/2006/08/26/obituaries/26mccullum.html': {'age': 93},
    '/2006/08/29/arts/music/leopold-simoneau-90-acclaimed-mozart-tenor-dies.html': {'age': 90},
    '/2006/08/30/world/europe/30BARZEL.html': {'gender': 'M', 'gender_src': 'manual', 'age': 82},
    '/2006/09/02/obituaries/02johnson.html': {'age': 94},
    '/2006/09/22/world/asia/22an.html': {'gender': 'M', 'gender_src': 'manual', 'age': 79},
    '/2006/09/28/world/asia/iva-toguri-daquino-known-as-tokyo-rose-and-later-convicted-of.html': {'age': 90},
    '/2006/10/05/us/05apple.html': {'gender': 'M', 'gender_src': 'manual', 'age': 71},
    '/2006/10/15/us/15bennett.html': {'gender': 'F', 'gender_src': 'manual', 'age': 71},
    '/2006/10/31/world/africa/01bothacnd.html': {'gender': 'M', 'gender_src': 'manual', 'age': 90},
    '/2006/11/01/sports/baseball/silas-simmons-111-veteran-of-baseballs-negro-leagues-is.html': {'age': 111},
    '/2006/11/01/world/africa/p-w-botha-defender-of-apartheid-is-dead-at-90.html': {'gender': 'M', 'gender_src': 'manual', 'age': 90},
    '/2006/11/03/obituaries/03duprat.html': {'gender': 'M', 'gender_src': 'manual', 'age': 74},
    '/2006/11/12/nyregion/12barmash.html': {'gender': 'M', 'gender_src': 'manual', 'age': 84},
    '/2006/12/18/technology/c-peter-mccolough-86-dies-led-xerox-to-prominence-in-13-years-as.html': {'age': 86},
    '/2007/03/16/sports/baseball/16kuhn.html': {'gender': 'M', 'gender_src': 'manual', 'age': 80},
    '/2007/03/31/business/31sticht.html': {'age': 89},
    '/2008/12/07/nyregion/07vonbulow.html': {'gender': 'F', 'gender_src': 'manual', 'age': 76},
    '/2009/05/23/sports/hockey/23smith.html': {'age': 95},
    '/2009/08/12/sports/baseball/12mantle.html': {'gender': 'F', 'gender_src': 'manual', 'age': 77},
    '/2010/04/10/movies/10raabe.html': {'age': 94},
    '/2010/11/29/us/29chance.html': {'age': 97},
    '/2013/07/31/sports/basketball/ossie-schectman-who-scored-the-nbas-first-points-dies-at-94.html': {'age': 94},
    '/2014/08/18/us/sophie-masloff-ex-mayor-of-pittsburgh-dies-at-96.html': {'age': 96},
    '/2015/05/20/nyregion/happy-rockefeller-whose-marriage-to-governor-scandalized-voters-dies-at-88.html': {'age': 88},
    '/2016/10/14/us/yutaka-yoshida-dead.html': {'gender': 'M', 'gender_src': 'manual', 'age': 104},
    '/2017/02/16/world/asia/ren-xinmin-dead-china-rockets.html': {'gender': 'M', 'gender_src': 'manual', 'age': 101},
    '/2017/07/21/books/clancy-sigal-dead-author-of-going-away.html': {'gender': 'M', 'gender_src': 'manual', 'age': 90},
    '/2018/03/08/obituaries/overlooked-lillias-campbell-davidson.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2018/03/28/obituaries/overlooked-yu-gwan-sun.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2018/04/18/obituaries/overlooked-harriott-daley.html': {'gender': 'F', 'gender_src': 'manual'},
    '/2018/07/06/obituaries/gudrun-burwitz-ever-loyal-daughter-of-himmler-is-dead-at-88.html': {'gender': 'F', 'gender_src': 'manual', 'age': 88},
    '/2018/09/05/obituaries/jan-ellen-lewis-expert-on-jeffersons-other-family-dies-at-69.html': {'gender': 'F', 'gender_src': 'manual', 'age': 69},
    '/2018/09/26/obituaries/voltairine-de-cleyre-overlooked.html': {'gender': 'F', 'gender_src': 'manual', 'age': 45},
    '/2018/10/17/obituaries/yamei-kin-overlooked.html': {'gender': 'F', 'gender_src': 'manual', 'age': 70},
    '/2018/11/14/obituaries/pandita-ramabai-overlooked.html': {'gender': 'F', 'gender_src': 'manual', 'age': 63},
    '/2019/02/27/obituaries/dondi-donald-joseph-white-overlooked.html': {'gender': 'M', 'gender_src': 'manual', 'age': 37},
    '/2019/07/12/science/rene-favaloro-dead.html': {'gender': 'M', 'gender_src': 'manual', 'age': 77},
    '/2019/11/27/arts/evelyne-daitz-dies-at-83-ran-a-vital-photography-gallery.html': {'gender': 'F', 'gender_src': 'manual', 'age': 83},
    '/2020/04/03/arts/harriet-glickman-dead-peanuts.html': {'gender': 'F', 'gender_src': 'manual', 'age': 93},
    '/2020/08/07/obituaries/jovita-idar-overlooked.html': {'gender': 'F', 'gender_src': 'manual', 'age': 60},
    '/2020/08/23/books/mercedes-barcha-dead.html': {'gender': 'F', 'gender_src': 'manual', 'age': 87},
    '/2020/08/23/obituaries/dr-alyce-gullattee-dead-coronavirus.html': {'gender': 'F', 'gender_src': 'manual', 'age': 91},
    '/2020/11/05/obituaries/arolde-de-oliveira-dead-coronavirus.html': {'gender': 'M', 'gender_src': 'manual', 'age': 83},
    '/2021/05/29/us/sister-margherita-marchione-dead.html': {'gender': 'F', 'gender_src': 'manual', 'age': 99},
    '/2021/07/13/books/priscilla-mcmillan-dead.html': {'gender': 'F', 'gender_src': 'manual', 'age': 92},
    '/2023/05/08/science/theodor-diener-dead.html': {'gender': 'M', 'gender_src': 'manual', 'age': 102},
    '/2024/04/12/obituaries/lizzie-magie-overlooked.html': {'gender': 'F', 'gender_src': 'manual', 'age': 81},
    '/2025/04/27/us/politics/alexis-herman-dead.html': {'gender': 'F', 'gender_src': 'manual', 'age': 77},
    '/2025/06/06/arts/jillian-sackler-dead.html': {'gender': 'F', 'gender_src': 'manual', 'age': 84},
    '/2025/07/24/sports/hulk-hogan-dead.html': {'gender': 'M', 'gender_src': 'manual', 'age': 71},
    '/2025/09/28/arts/music/viv-prince-dead.html': {'gender': 'M', 'gender_src': 'manual', 'age': 84},
    '/2025/10/16/world/asia/kanchha-sherpa-mt-everest-dead.html': {'gender': 'M', 'gender_src': 'manual', 'age': 92},
    '/interactive/2016/01/13/obituaries/von-braun-obits.html': {'gender': 'M', 'gender_src': 'manual'},
    '/interactive/2016/06/30/obituaries/sinclair.html': {'gender': 'M', 'gender_src': 'manual'},
    # ---- 2026-04-27 user-flagged name parser failures ----
    # Reagan: all-caps "FOSTERED COLD-WAR MIGHT…" segment fooled parser.
    '/2004/06/06/us/ronald-reagan-dies-at-93-fostered-cold-war-might-and-curbs-on-government.html': {
        'name': 'Ronald Reagan', 'gender': 'M', 'gender_src': 'manual', 'age': 93,
    },
    # Solzhenitsyn: slug "04solzhenitsyn" has no first name; parser kept just last name.
    '/2008/08/04/books/04solzhenitsyn.html': {
        'name': 'Aleksandr Solzhenitsyn', 'gender': 'M', 'gender_src': 'manual', 'age': 89,
    },
    # John Paul Stevens: parser kept "Supreme Court Justice" prefix.
    '/2019/07/16/us/john-paul-stevens-dead.html': {
        'name': 'John Paul Stevens', 'gender': 'M', 'gender_src': 'manual', 'age': 99,
    },
    # Cartier-Bresson: hyphenated last name; parser captured only the surname.
    '/2004/08/05/arts/cartier-bresson-artist-who-used-lens-dies-at-95.html': {
        'name': 'Henri Cartier-Bresson', 'gender': 'M', 'gender_src': 'manual', 'age': 95,
    },
    # Moynihan: "Former Senator" prefix swallowed the name.
    '/2003/03/26/obituaries/former-senator-daniel-patrick-moynihan-dead-at-76.html': {
        'name': 'Daniel Patrick Moynihan', 'gender': 'M', 'gender_src': 'manual', 'age': 76,
    },
    # Isaac Stern: "Violinist" profession-prefix kept in name.
    '/2001/09/23/nyregion/violinist-isaac-stern-dies-at-81-led-efforts-to-save-carnegie-hall.html': {
        'name': 'Isaac Stern', 'gender': 'M', 'gender_src': 'manual', 'age': 81,
    },
    # Buchwald: numeric-prefix slug + "Whose…" func-word foiled parser.
    '/2007/01/19/obituaries/19buchwald.html': {
        'name': 'Art Buchwald', 'gender': 'M', 'gender_src': 'manual', 'age': 81,
    },
    # Pope John Paul II: the April 4 stub (698w) is a news brief. Name set
    # to 'John Paul II' so it clusters with the 13,870w April 3 essay obit
    # (which has the same name via ESSAY_OBIT_URLS + OBIT_OVERRIDES). The
    # dedup pass will pick the longer entry as primary and attach this URL
    # as a secondary_url — so it's linked but subservient.
    '/2005/04/04/world/europe/obituary-karol-wojtyla-19202005.html': {
        'name': 'John Paul II', 'gender': 'M', 'gender_src': 'manual', 'age': 84,
        'profession': 'Pope',
    },
    # Qaddafi: section=Obituaries, but headline is "An Erratic Leader…" with
    # no death verb. URL slug "qaddafi-killed-…" now matches the URL hint.
    '/2011/10/21/world/africa/qaddafi-killed-as-hometown-falls-to-libyan-rebels.html': {
        'name': 'Muammar el-Qaddafi', 'gender': 'M', 'gender_src': 'manual', 'age': 69,
    },
    # Hugo Chavez: tom='Obituary (Obit)' but headline is essay-style
    # ('A Polarizing Figure Who Led a Movement') with no name.
    '/2013/03/06/world/americas/hugo-chavez-venezuelas-polarizing-leader-dies-at-58.html': {
        'name': 'Hugo Chávez', 'gender': 'M', 'gender_src': 'manual', 'age': 58,
    },
    # Helen Keller Insider feature: slug 'document-Helenkeller' is one
    # concatenated word; parser produced 'Document Helenkeller'.
    '/interactive/2016/06/01/obituaries/document-Helenkeller.html': {
        'name': 'Helen Keller', 'gender': 'F', 'gender_src': 'manual', 'age': 87,
        'profession': 'author and disability advocate',
    },
    # Dale Earnhardt: short crash-news headline ("Earnhardt Dies in Crash")
    # has no first name. Race-car driver, killed at Daytona.
    '/2001/02/19/sports/earnhardt-dies-in-crash.html': {
        'name': 'Dale Earnhardt', 'age': 49, 'profession': 'NASCAR Driver',
        'gender': 'M', 'gender_src': 'manual',
    },
    # ---- 2026-04-27 unparsed.xlsx batch (gender + name/age/profession patches) ----
    '/2000/09/08/nyregion/t-l-deglin-92-public-relations-executive.html': {'name': 'T. L. Deglin', 'age': 92, 'gender': 'M', 'gender_src': 'manual'},
    '/2000/10/13/world/hendrik-casimir-90-theorist-in-study-of-quantum-mechanics.html': {'name': 'Hendrik Casimir', 'age': 90, 'gender': 'M', 'gender_src': 'manual'},
    '/2001/03/13/us/s-dillon-ripley-dies-87-led-smithsonian-institution-during-its-greatest-growth.html': {'name': 'S. Dillon Ripley', 'age': 87, 'gender': 'M', 'gender_src': 'manual'},
    '/2001/12/15/nyregion/a-keith-smiley-91-executive-of-resort-and-preservationist.html': {'name': 'A. Keith Smiley', 'age': 91, 'gender': 'M', 'gender_src': 'manual'},
    '/2002/07/14/world/yousuf-karsh-who-photographed-famous-and-infamous-of-20th-century-dies-at-93.html': {'name': 'Yousuf Karsh', 'age': 93, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Photographed Famous And Infamous of 20th Century'},
    '/2003/03/23/us/col-edson-raff-95-dies-led-paratroopers-in-1942.html': {'name': 'Col. Edson Raff', 'age': 95, 'gender': 'M', 'gender_src': 'manual'},
    '/2003/09/11/nyregion/m-konvitz-scholar-of-law-and-idealism-is-dead-at-95.html': {'name': 'Milton R. Konvitz', 'age': 95, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Scholar of Law And Idealism'},
    '/2004/01/06/business/takashi-ishihara-91-dies-led-nissan-s-rise.html': {'name': 'Takashi Ishihara', 'age': 91, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/01/13/nyregion/perry-b-duryea-former-assembly-speaker-dies-at-82.html': {'name': 'Perry B. Duryea', 'age': 82, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Former Assembly Speaker'},
    '/2004/01/29/news/obituary-salvador-laurel-of-philippines.html': {'name': 'Salvador Laurel', 'age': 76, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/02/04/nyregion/adella-wotherspoon-last-survivor-of-general-slocum-disaster-is-dead-at-100.html': {'name': 'Adella Wotherspoon', 'age': 100, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Last Survivor of General Slocum Disaster'},
    '/2004/02/06/nyregion/trude-wenzel-lash-95-an-advocate-for-children.html': {'name': 'Trude Wenzel Lash', 'age': 95, 'gender': 'F', 'gender_src': 'manual'},
    '/2004/03/16/news/obituary-arky-gonzalez-75-writer.html': {'name': 'Arky Gonzalez', 'age': 75, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/06/07/nyregion/m-searle-wright-86-teacher-composer-and-organ-expert.html': {'name': 'M. Searle Wright', 'age': 86, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/06/15/nyregion/whitman-knapp-95-dies-exposed-police-corruption.html': {'name': 'Whitman Knapp', 'age': 95, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/07/07/us/tf-mancuso-who-led-radiation-study-dies-at-92.html': {'name': 'T.F. Mancuso', 'age': 92, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Led Radiation Study'},
    '/2004/07/12/business/laurance-s-rockefeller-passionate-conservationist-and-investor-is-dead-at-94.html': {'name': 'Laurance S. Rockefeller', 'age': 94, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Passionate Conservationist And Investor'},
    '/2004/07/12/theater/phoebe-brand-96-actress-and-group-theater-co-founder.html': {'name': 'Phoebe Brand', 'age': 96, 'gender': 'F', 'gender_src': 'manual'},
    '/2004/09/22/us/w-c-reeves-crucial-ally-in-west-nile-fight-dies-at-87.html': {'name': 'W. C. Reeves', 'age': 87, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Crucial Ally in West Nile Fight'},
    '/2004/09/26/obituaries/w-dorwin-teague-94-industrial-designer-is-dead.html': {'name': 'W. Dorwin Teague', 'age': 94, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/09/30/books/mulk-raj-anand-99-famed-indian-writer-dies.html': {'name': 'Mulk Raj Anand', 'age': 99, 'gender': 'M', 'gender_src': 'manual'},
    '/2004/12/24/news/obituary-willet-weeks-jr-executive-for-paris-newspaper-in-1950s.html': {'name': 'Willet Weeks Jr.', 'age': 87, 'gender': 'M', 'gender_src': 'manual', 'profession': 'executive for Paris newspaper in 1950s'},
    '/2005/01/06/obituaries/maclyn-mccarty-dies-at-93-pioneer-in-dna-research.html': {'name': 'Maclyn McCarty', 'age': 93, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/01/09/obituaries/j-n-dixit-68-dies-served-as-indias-negotiator-in-pakistan-and.html': {'name': 'J. N. Dixit', 'age': 68, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/01/20/science/h-bentley-glass-provocative-science-theorist-dies-at-98.html': {'name': 'H. Bentley Glass', 'age': 98, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Provocative Science Theorist'},
    '/2005/01/28/obituaries/cordelia-scaife-may-76-a-mellon-heir-dies.html': {'name': 'Cordelia Scaife May', 'age': 76, 'gender': 'F', 'gender_src': 'manual'},
    '/2005/02/06/obituaries/boce-w-barlow-jr-89-judge-and-senator-is-dead.html': {'name': 'Boce W. Barlow Jr.', 'age': 89, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/03/10/theater/trude-rittmann-an-arranger-of-broadway-favorites-dies-at-96.html': {'name': 'Trude Rittmann', 'age': 96, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Arranger of Broadway Favorites'},
    '/2005/03/23/arts/design/kenzo-tange-architect-of-urban-japan-dies-at-91.html': {'name': 'Kenzo Tange', 'age': 91, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Architect of Urban Japan'},
    '/2005/04/15/college/andr-franois-is-dead-at-89-illustrator-with-biting-satire.html': {'name': 'André François', 'age': 89, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/04/30/sports/hockey/red-horner-one-of-hockeys-toughest-players-dies-at-95.html': {'name': 'Red Horner', 'age': 95, 'gender': 'M', 'gender_src': 'manual', 'profession': 'One of Hockeys Toughest Players'},
    '/2005/07/23/arts/music/blue-barron-91-bigband-leader-dies.html': {'name': 'Blue Barron', 'age': 91, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/07/27/us/a-william-holmberg-jr-former-newspaper-executive-dies-at-81.html': {'name': 'A. William Holmberg Jr.', 'age': 81, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/08/02/world/europe/obituary-wim-duisenberg-candid-first-president-of-european.html': {'name': 'Wim Duisenberg', 'age': 70, 'gender': 'M', 'gender_src': 'manual', 'profession': 'candid first president of European Central Bank'},
    '/2005/10/05/world/europe/obituaries-ronnie-barker-patrick-caulfield.html': {'name': 'Ronnie Barker', 'age': 76, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Patrick Caulfield'},
    '/2005/10/12/world/africa/obituaries-milton-obote-of-uganda-louis-nye.html': {'name': 'Milton Obote', 'age': 80, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Louis Nye'},
    '/2005/10/18/world/europe/obituary-a-yakovlev-champion-of-soviet-change-81.html': {'name': 'Alexander Yakovlev', 'age': 81, 'gender': 'M', 'gender_src': 'manual', 'profession': 'champion of Soviet change'},
    '/2005/11/04/nyregion/waldemar-nielsen-expert-on-philanthropy-dies-at-88.html': {'name': 'Waldemar Nielsen', 'age': 88, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Expert on Philanthropy'},
    '/2005/11/12/business/peter-f-drucker-a-pioneer-in-social-and-management-theory-is-dead.html': {'name': 'Peter F. Drucker', 'age': 95, 'gender': 'M', 'gender_src': 'manual'},
    '/2005/11/27/world/asia/obituary-gopal-godse-86-conspired-to-kill-gandhi.html': {'name': 'Gopal Godse', 'age': 86, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/01/20/world/americas/obituary-colonel-edward-hall-led-efforts-for-intercontinental.html': {'name': 'Col. Edward N. Hall', 'gender': 'M', 'gender_src': 'manual', 'profession': 'led efforts for intercontinental missiles'},
    '/2006/01/27/world/europe/obituary-johannes-rau-75-led-germany.html': {'name': 'Johannes Rau', 'age': 75, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/01/31/arts/obituary-video-artist-nam-june-paik-dies-at-74.html': {'name': 'Nam June Paik', 'age': 74, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/03/12/world/europe/slobodan-milosevic-64-former-yugoslav-leader-accused-of-war.html': {'name': 'Slobodan Milosevic', 'age': 64, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/03/29/world/europe/obituaries-stig-wennerstrom-swedish-spy.html': {'name': 'Stig Wennerstrom', 'age': 99, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Swedish spy'},
    '/2006/04/02/nyregion/matt-kennedy-101-dies-stalwart-of-coney-island.html': {'name': 'Matt Kennedy', 'age': 101, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/04/12/style/bobbie-nudie-purveyor-of-glitter-to-rhinestone-cowboys-dies-at-92.html': {'name': 'Bobbie Nudie', 'age': 92, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Purveyor of Glitter to Rhinestone Cowboys'},
    '/2006/04/13/world/americas/obituaries-ws-coffin-81-activist-and-yale-chaplain.html': {'name': 'W.S. Coffin', 'age': 81, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/05/01/books/01prem.html': {'name': 'Pramoedya Ananta Toer', 'age': 81, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/05/02/world/europe/02revel.html': {'name': 'Jean-François Revel', 'age': 82, 'gender': 'M', 'gender_src': 'manual', 'profession': 'French Philosopher'},
    '/2006/06/01/arts/01aarons.html': {'name': 'Slim Aarons', 'age': 89, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/07/12/arts/12hughes.html': {'name': 'Barnard Hughes', 'age': 90, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Character Actor'},
    '/2006/07/27/us/27mosteller.html': {'name': 'C. Frederick Mosteller', 'age': 89, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Pioneer in Statistics'},
    '/2006/08/26/obituaries/26mccullum.html': {'name': 'Vashti McCollum', 'age': 93, 'gender': 'F', 'gender_src': 'manual'},
    '/2006/08/29/arts/music/leopold-simoneau-90-acclaimed-mozart-tenor-dies.html': {'name': 'Léopold Simoneau', 'age': 90, 'gender': 'M', 'gender_src': 'manual'},
    # Final 6 unparsed-gender records (alternate URLs for subjects covered above)
    '/2005/11/13/world/americas/obituary-management-guru-peter-f-drucker-dies.html': {'name': 'Peter F. Drucker', 'age': 95, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Management Guru'},
    '/2006/03/13/world/europe/obituary-serbian-nationalist-leader-ignited-balkan-wars-of.html': {'name': 'Slobodan Milosevic', 'age': 64, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Serbian Nationalist Leader'},
    '/2006/04/30/world/asia/obituary-renowned-indonesian-author-pramoedya-ananta-toer-dies.html': {'name': 'Pramoedya Ananta Toer', 'age': 81, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Indonesian Author'},
    '/2006/05/02/world/europe/jf-revel-french-philosopher-is-dead-at-82.html': {'name': 'Jean-François Revel', 'age': 82, 'gender': 'M', 'gender_src': 'manual', 'profession': 'French Philosopher'},
    '/2006/11/13/nyregion/obituaries/isadore-barmash-84-prolific-chronicler-of-retail-wars.html': {'name': 'Isadore Barmash', 'age': 84, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Chronicler of Retail Wars'},
    '/2006/12/06/obituaries/rosie-lee-tompkins-africanamerican-quiltmaker-dies-at-70.html': {'name': 'Rosie Lee Tompkins', 'age': 70, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Quiltmaker'},
    '/2006/09/02/obituaries/02johnson.html': {'name': 'Buffie Johnson', 'age': 94, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Artist and Friend of Artists'},
    '/2006/09/28/world/asia/iva-toguri-daquino-known-as-tokyo-rose-and-later-convicted-of.html': {'name': "Iva Toguri D'Aquino", 'age': 90, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Known as Tokyo Rose and Later Convicted of Treason'},
    '/2006/11/01/sports/baseball/silas-simmons-111-veteran-of-baseballs-negro-leagues-is.html': {'name': 'Silas Simmons', 'age': 111, 'gender': 'M', 'gender_src': 'manual'},
    '/2006/12/18/technology/c-peter-mccolough-86-dies-led-xerox-to-prominence-in-13-years-as.html': {'name': 'C. Peter McColough', 'age': 86, 'gender': 'M', 'gender_src': 'manual'},
    '/2007/03/31/business/31sticht.html': {'name': 'J. Paul Sticht', 'age': 89, 'gender': 'M', 'gender_src': 'manual'},
    '/2009/05/23/sports/hockey/23smith.html': {'name': 'Clint Smith', 'age': 95, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Won Stanley Cup With Rangers'},
    '/2010/04/10/movies/10raabe.html': {'name': 'Meinhardt Raabe', 'age': 94, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Famous Munchkin'},
    '/2010/11/29/us/29chance.html': {'name': 'Britton Chance', 'age': 97, 'gender': 'M', 'gender_src': 'manual', 'profession': 'Olympian and Biophysics Researcher'},
    '/2013/07/31/sports/basketball/ossie-schectman-who-scored-the-nbas-first-points-dies-at-94.html': {'name': 'Ossie Schectman', 'age': 94, 'gender': 'M', 'gender_src': 'manual', 'profession': 'N.B.A.s First Scorer'},
    '/2014/01/06/sports/soccer/eusebio-71-legend-of-portuguese-soccer-dies.html': {'name': 'Eusébio', 'age': 71, 'gender': 'M', 'gender_src': 'manual'},
    '/2014/08/18/us/sophie-masloff-ex-mayor-of-pittsburgh-dies-at-96.html': {'name': 'Sophie Masloff', 'age': 96, 'gender': 'F', 'gender_src': 'manual', 'profession': 'Ex-Mayor of Pittsburgh'},
    '/2015/05/20/nyregion/happy-rockefeller-whose-marriage-to-governor-scandalized-voters-dies-at-88.html': {'name': 'Happy Rockefeller', 'age': 88, 'gender': 'F', 'gender_src': 'manual'},
    # Charlotta Bass: Overlooked headline "Before Kamala Harris, There Was
    # Charlotta Bass" got split on the comma — name parsed as "Before Kamala
    # Harris", profession as "There Was Charlotta Bass".
    '/2020/09/04/obituaries/charlotta-bass-vice-president-overlooked.html': {
        'name': 'Charlotta Bass', 'profession': 'First Black Woman to Run for Vice President',
        'gender': 'F', 'gender_src': 'manual',
    },
    # Yousra Abdel Raouf al-Kidwa: headline "Arafat's Sister, 77, Dies in a
    # Cairo Hospital" carries no name — the parser captured the descriptor.
    '/2003/08/14/world/arafat-s-sister-77-dies-in-a-cairo-hospital.html': {
        'name': 'Yousra Abdel Raouf al-Kidwa', 'profession': "Yasser Arafat's Sister",
    },
    # Leonard L. Farber: headline "Leonard L. Farber Shopping Mall Executive,
    # Dies at 89" was split on the wrong comma — name absorbed the descriptor.
    '/2005/08/08/business/leonard-l-farber-shopping-mall-executive-dies-at-89.html': {
        'name': 'Leonard L. Farber', 'profession': 'Shopping Mall Executive',
    },
    # Archbishop Christodoulos: headline "Greek Orthodox Leader Dies at 69" gives
    # no parseable name — override both URL variants (same article, two API entries).
    '/2008/01/29/world/europe/29christodoulos.html': {
        'name': 'Christodoulos', 'display_name': 'Archbishop Christodoulos',
        'gender': 'M', 'gender_src': 'honorific', 'profession': 'Greek Orthodox Archbishop',
    },
    '/2008/01/29/world/europe/29greece.html': {
        'name': 'Christodoulos', 'display_name': 'Archbishop Christodoulos',
        'gender': 'M', 'gender_src': 'honorific', 'profession': 'Greek Orthodox Archbishop',
    },
    # Essay-style obits added to ESSAY_OBIT_URLS — also override names/metadata
    # since their headlines are descriptive rather than name-first.
    '/2005/04/03/world/europe/allembracing-man-of-action-for-a-new-era-of-papacy.html': {
        'name': 'John Paul II', 'display_name': 'Pope John Paul II',
        'gender': 'M', 'gender_src': 'manual', 'age': 84, 'profession': 'Pope',
    },
    '/2011/03/27/us/politics/27geraldine-ferraro.html': {
        'name': 'Geraldine Ferraro', 'display_name': 'Geraldine A. Ferraro',
        'gender': 'F', 'gender_src': 'pronoun', 'age': 75,
        'profession': 'First Female Major-Party V.P. Nominee',
    },
    '/2010/12/14/world/14holbrooke.html': {
        'name': 'Richard Holbrooke', 'display_name': 'Richard C. Holbrooke',
        'gender': 'M', 'gender_src': 'pronoun', 'age': 69,
        'profession': 'Diplomat and Special Envoy',
    },
    '/2010/12/08/us/08edwards.html': {
        'name': 'Elizabeth Edwards',
        'gender': 'F', 'gender_src': 'pronoun', 'age': 61,
        'profession': 'Political Activist and Author',
    },
}

# Multi-subject obituaries: one URL covers two or more deaths (spouses,
# siblings, co-victims). Emit one record per subject; all share the same URL
# and date. The dict is keyed by URL; each value is a list of override dicts
# applied on top of the parsed obit.
OBIT_SPLITS = {
    '/2003/01/17/us/2-archaeologists-robert-braidwood-95-and-his-wife-linda-braidwood-93-die.html': [
        {'name': 'Robert J. Braidwood', 'age': 95, 'gender': 'M', 'gender_src': 'manual',
         'profession': 'archaeologist'},
        {'name': 'Linda S. Braidwood',  'age': 93, 'gender': 'F', 'gender_src': 'manual',
         'profession': 'archaeologist'},
    ],
    # ---- 2026-04-25 manual review (xlsx) — multi-subject obituaries ----
    '/2018/04/11/obituaries/overlooked-lin-huiyin-and-liang-sicheng.html': [
        {'name': 'Lin Huiyin', 'age': 51, 'gender': 'F', 'gender_src': 'manual'},
        {'name': 'Liang Sicheng', 'age': 70, 'gender': 'M', 'gender_src': 'manual'},
    ],
    '/2018/06/08/obituaries/gremina-and-ugarov-russia-teatr-doc-die.html': [
        {'name': 'Mikhail Ugarov', 'age': 62, 'gender': 'M', 'gender_src': 'manual'},
        {'name': 'Elena Gremina', 'age': 61, 'gender': 'F', 'gender_src': 'manual'},
    ],
    '/2020/05/18/obituaries/cleon-and-leon-boyd-dead-coronavirus.html': [
        {'name': 'Cleon Boyd', 'age': 64, 'gender': 'M', 'gender_src': 'manual'},
        {'name': 'Leon Boyd', 'age': 64, 'gender': 'M', 'gender_src': 'manual'},
    ],
    '/2020/12/21/obituaries/rosendo-rogelio-mendoza-dead.html': [
        {'name': 'Rosendo Mendoza', 'age': 56, 'gender': 'M', 'gender_src': 'manual'},
        {'name': 'Rogelio Mendoza', 'age': 56, 'gender': 'M', 'gender_src': 'manual'},
    ],
    '/2022/04/03/arts/music/mighty-diamonds-dead.html': [
        {'name': 'Tabby Diamond', 'age': 66, 'gender': 'M', 'gender_src': 'manual'},
        {'name': 'Bunny Diamond', 'age': 70, 'gender': 'M', 'gender_src': 'manual'},
    ],
    '/2022/10/14/obituaries/katharine-briggs-and-isabel-myers-overlooked.html': [
        {'name': 'Katharine Briggs', 'age': 93, 'gender': 'F', 'gender_src': 'manual'},
        {'name': 'Isabel Myers', 'age': 82, 'gender': 'F', 'gender_src': 'manual'},
    ],

    # ---- 2026-04-27 manual review (Corrected obits.xlsx) ----
    '/2022/01/07/world/europe/grichka-and-igor-bogdanoff-dead.html': [
        {'name': 'Grichka Bogdanoff', 'age': 72, 'gender': 'M', 'gender_src': 'manual'},
        {'name': 'Igor Bogdanoff', 'age': 72, 'gender': 'M', 'gender_src': 'manual'},
    ],
    '/2022/09/23/us/ilse-nathan-ruth-siegler-scheuer-dead.html': [
        {'name': 'Ilse Nathan', 'age': 98, 'gender': 'F', 'gender_src': 'manual'},
        {'name': 'Ruth Siegler', 'age': 95, 'gender': 'F', 'gender_src': 'manual'},
    ],
}
# 9/11 "Portraits of Grief" — published Dec 2001 - Sep 2002, ~1,800 articles.
# Profile-style obits: headline is "Name: Tagline", desk='National / Portraits
# of Grief', URL contains '/national/portraits/'. Standard parsers fail
# because they have no death verb in the headline.
RE_PORTRAITS_URL = re.compile(r'/national/portraits/', re.I)
RE_PORTRAITS_DESK = re.compile(r'Portraits of Grief', re.I)
RE_PORTRAITS_HEADLINE = re.compile(r'^([A-Z][\w.\'\-\u00C0-\u017F]+(?:\s+[A-Z][\w.\'\-\u00C0-\u017F]+){0,4}):\s*\S')
# Headlines that mark group / multi-subject / package pieces, not single obits
RE_GROUP_HEADLINE = re.compile(
    r'^(?:Lesson of the Day|The Lives They Lived|Year in Review|'
    r'In a Political Year|In a Year of|Among Deaths in|Obituaries: Deaths in|'
    r'Notable Deaths|Notable Obits|Those We|'
    r'In Remembrance|In Memoriam:|'
    # Year-end roundups: "Deaths in 2017: …", "Deaths of 2021: …",
    # "Gone in 2025", "What They Left Behind"
    r'Deaths (?:in|of) \d{4}\b|Gone in \d{4}\b|'
    r'What They Left Behind)\b',
    re.I,
)


def extract_name_from_slug(url):
    """Extract a candidate name from URL slug as last-resort fallback.
    /2021/06/08/sports/football/jim-fassel-giants-dead.html → "Jim Fassel"
    /2022/08/12/us/anne-heche-brain-injury.html → "Anne Heche"
    /2007/07/01/nyregion/01mancuso.html → None (single token, no first name)
    Returns None if the slug yields fewer than 2 plausible name tokens.
    """
    if not url: return None
    # Get terminal slug (path component before .html), drop leading date digits
    m = re.search(r'/([a-z0-9][a-z0-9\-]+?)(?:\.html?)?$', url, re.I)
    if not m: return None
    slug = m.group(1).lower()
    slug = re.sub(r'^\d+[-_]?', '', slug)               # drop leading "01" etc.
    slug = re.sub(r'(?:[-_](?:dead|dies|obituary|obit|overlooked))+$', '', slug)
    # Strip leading series tokens — "/interactive/2018/obituaries/overlooked-
    # diane-arbus.html" should yield "Diane Arbus", not "Overlooked Diane".
    slug = re.sub(r'^(?:overlooked(?:-no-more)?|not-forgotten|portraits-of-grief|'
                  r'in-memoriam|lives-they-lived|what-they-left-behind)[-_]+',
                  '', slug)
    # Strip honorific title prefixes from slug — "dr-art-winfree-..." should
    # yield "Art Winfree", not "Dr Art". The title is preserved on the obit
    # via make_display_name (which reads the original headline), so the
    # internal `name` field stays canonical.
    slug = re.sub(r'^(?:dr|rev|reverend|sister|brother|father|sir|dame|'
                  r'lord|lady|justice|judge|sen|senator|rep|representative|'
                  r'gov|governor|capt|captain|col|colonel|gen|general|'
                  r'lt|lieutenant|sgt|sergeant|cardinal|bishop|prof|professor|'
                  r'mr|mrs|ms|mx|mayor|president)[-_]+',
                  '', slug, flags=re.I)
    parts = [p for p in re.split(r'[-_]+', slug)
             if p and not p.isdigit() and len(p) > 1
             and p not in {'cnd', 'and', 'the', 'at', 'on', 'in', 'of', 'web', 'index'}]
    if len(parts) < 2: return None
    # Take first 2-3 tokens as the name guess. Stop if we hit a token that
    # looks like noise ("giants", "uruguay", "brain") — i.e., if a token
    # doesn't look like a typical surname and we already have 2 tokens, stop.
    name_parts = parts[:2]
    if len(parts) >= 3 and len(parts[2]) >= 4 and parts[2][0].isalpha():
        # Add third token only if it could plausibly be part of a name
        # (e.g. middle names, titles). Heuristic: short third token unlikely.
        # Conservative — don't add for now to avoid "Jim Fassel Giants".
        pass
    return ' '.join(p.capitalize() for p in name_parts)


def extract_name_from_abstract(abstract):
    """Fallback: extract name from abstract when headline parsing fails.

    Abstracts very often follow "Full Name, age, verb..." or "Full Name,
    a [role] who..." \u2014 the name is the leading capitalized phrase before
    the first comma that is followed by a digit (age) or article (a/an/the).
    Also handles "Mr./Mrs./Dr. Full Name" patterns.
    """
    if not abstract:
        return None
    ab = abstract.strip()
    # Strip honorific so "Mr. John Smith, 75," \u2192 "John Smith, 75,"
    ab_clean = RE_LEADING_TITLE.sub('', ab)
    # Pattern: "Name, age," or "Name, a/an/the role"
    m = re.match(
        r'^([A-Z][a-zA-Z\u00c0-\u00ff.\'\-]+(?: [A-Z][a-zA-Z\u00c0-\u00ff.\'\-]+){1,5})'
        r',\s*(?:\d{2,3}|[Aa]n? |[Tt]he )',
        ab_clean
    )
    if m:
        cand = m.group(1).strip()
        if 1 <= cand.count(' ') <= 5 and not looks_descriptive(cand):
            return cand
    return None


def extract_name(headline, url=None, is_portraits=False):
    if not headline: return None
    # Drop zero-width chars
    h = headline.replace('\u200b', '').replace('\ufeff', '').strip()
    # Portraits of Grief: headline is "Name: Tagline" — colon-separated.
    if is_portraits:
        m = RE_PORTRAITS_HEADLINE.match(h)
        if m:
            cand = m.group(1).strip()
            if 1 <= cand.count(' ') <= 4:
                return cand
    # Strip "From YYYY:" republished-obit prefix (apply before series check
    # since some headlines stack "From 1992: Marlene Dietrich Is Dead")
    h = RE_FROM_YEAR.sub('', h)
    # Strip recurring series prefixes (Overlooked No More, Not Forgotten, etc.)
    h = RE_LEADING_SERIES.sub('', h)
    # Strip "Killed at 71," / "Dead at 89," prefixes that put the verb phrase
    # in front of the name. ("Killed at 71, Ayman al-Zawahri Led a Life…")
    h = RE_LEADING_DEATH_PREFIX.sub('', h)
    # Strip parentheticals — "Barry Humphries (Dame Edna to You, Possums) Is
    # Dead at 89" should parse as "Barry Humphries Is Dead at 89".
    h = re.sub(r'\s*\([^)]*\)', '', h)
    # Strip "; description" or " -- description" trailing modifiers
    # ("Bertina Carter Hunter; Arts Patron" → "Bertina Carter Hunter")
    h = re.sub(r'\s*[;\u2014\u2013]\s.*$|\s+--\s.*$', '', h)
    # Strip bare "Former [role]" prefix when it precedes the actual name.
    # RE_LEADING_TITLE only strips "Former" when followed by a real title
    # (General, Senator, etc.). This catches "Former Giants Linebacker Brad…"
    h = re.sub(r'^(?:Former|Ex-|Late|Retired|Veteran)\s+(?:[A-Z][a-z]+\s+){0,2}(?=[A-Z])', '', h)
    # Strip hyphenated-nationality adjective: "African-American Golf Pioneer…"
    # → "Golf Pioneer…" (just the adjective; remaining role words are handled
    # by the possessive/tagline logic or leave enough for slug fallback).
    h = re.sub(
        r'^[A-Z][a-z]+-(?:American|Born|Based|Led|Owned|Run)\s+', '', h
    )
    # Strip possessive-word prefix: "Baseball's Herman Franks" →
    # "Herman Franks"; "Russia's Market Reform Architect" → drop whole phrase
    # (those are caught by the role-tagline fallback below). Only strip when
    # the word before 's is clearly a common noun (not a proper name like
    # "Ellington's sideman").
    h = re.sub(
        r"^(?:Baseball|Football|Soccer|Tennis|Basketball|Golf|Boxing|Wrestling"
        r"|Hollywood|Broadway|Television|Radio|Jazz|Rock|Pop|Hip-Hop|Country"
        r"|Opera|Ballet|Dance|Art|Literature|Science|Politics|Russia|China"
        r"|Britain|France|America|Europe|Africa|Asia)'s\s+",
        '', h, flags=re.I
    )
    # Strip leading "Long May He Reign:" / "In Loving Memory:" descriptive
    # prefixes that put a phrase before the name and a colon after.
    h = re.sub(r'^[A-Z][^,:]{5,80}:\s+(?=[A-Z])', '', h)
    # Strip leading "'Nickname': " prefixes
    h = re.sub(r'^[\u2018\u201C\'"][^\u2019\u201D\'"]+[\u2019\u201D\'"]\s*[:,]\s*', '', h)
    # Strip honorific titles (The Reverend, Sir, Dr., Representative, etc.)
    # Apply twice in case there are stacked titles ("Rep. Dr. ...").
    h = RE_LEADING_TITLE.sub('', h)
    h = RE_LEADING_TITLE.sub('', h)
    # Single-role-then-name: "Choreographer, Sophie Maslow, Dies at 95"
    # The first token is a capitalized profession/role (no spaces), followed
    # by the actual name between commas. Only fires when the leading word is
    # in DESCRIPTIVE_TOKENS (a safe signal that it's a role, not a name).
    role_name_m = re.match(
        r'^([A-Z][a-z]+),\s+'           # Role word,
        r'([A-Z][a-zA-ZÀ-ÿ.\'\-]+'     # First name
        r'(?:\s+[A-Z][a-zA-ZÀ-ÿ.\'\-]+){0,4})'  # additional tokens
        r',\s+(?:Dies?|Is\s+Dead|Was\s+Dead)',
        h
    )
    if role_name_m and role_name_m.group(1).lower() in DESCRIPTIVE_TOKENS:
        cand = role_name_m.group(2).strip()
        if 0 <= cand.count(' ') <= 4 and not looks_descriptive(cand):
            return cand
    # Tagline-then-name: "A Man of Many Words, David Shulman Dies at 91" →
    # the part before the comma is descriptive; the real name follows.
    m = RE_TAGLINE_NAME.match(h) or RE_TAGLINE_NAME_GENERIC.match(h)
    if m:
        cand = m.group(1).strip()
        if cand and 0 <= cand.count(' ') <= 5 and not looks_descriptive(cand):
            return cand
    # "Eben Pyne 89, Who Helped…" — name then space-separated age. Insert the
    # missing comma so the standard parser sees "Eben Pyne, 89, Who Helped…".
    m = RE_NAME_AGE_NO_COMMA.match(h)
    if m:
        h = h.replace(m.group(0), f'{m.group(1)}, {m.group(2)}, ', 1)
    m = RE_NAME_COMMA.match(h)
    if m:
        cand = m.group(1).strip()
        # Reject if "Is Dead" / "Dies at" appears inside the candidate — that
        # means the comma fell after the verb phrase, not after the name.
        # ("D. Avramovic Is Dead at 81; Reform…" wraps name in candidate text)
        if RE_HAS_DIES.search(cand):
            cand = RE_HAS_DIES.split(cand)[0].strip().rstrip(',;:.')
            cand = re.sub(r'\s+(?:Is|Has|Was)$', '', cand)
        # Reject obvious non-names
        if any(w in cand.lower() for w in (' was ', ' is ', ' has ', ' will ')): return None
        # Reject if it looks like a tagline (function words like "of", "with").
        # Real names don't contain " of " or " with ", except for the
        # particles we whitelist as part of names ("of" never is one).
        # If the candidate looks like a tagline (contains "of"/"with"/etc.), DON'T
        # return None — fall through to the leading-caps fallback so cases like
        # "Mary Travers of Peter, Paul and Mary Dies at 72" can still parse.
        if not RE_HAS_FUNC_WORD.search(cand):
            # Allow single-word names (Birendra, Cher, Madonna) up to 6-token compound names
            if cand and 0 <= cand.count(' ') <= 6 and not looks_descriptive(cand):
                return cand
    # Headlines with "Dies at N" but no comma (or comma falls after verb):
    # capture leading 1-4 capitalized tokens. Handles "Joe Moakley of
    # Massachusetts Dies at 74", "Derek Freeman Dies at 84".
    if RE_HAS_DIES.search(h):
        m = RE_LEADING_CAPS.match(h)
        if m:
            cand = m.group(1).strip()
            if not looks_descriptive(cand):
                return cand
    # Last resort: the headline still has a "Led/Was/Made" verb pointing to a
    # name (handles "Ayman al-Zawahri Led a Life…" after the prefix strip).
    if re.search(r'\b(?:Led|Was|Made|Built|Founded|Wrote|Helped|Played)\b', h):
        m = RE_LEADING_CAPS.match(h)
        if m:
            cand = m.group(1).strip()
            if not looks_descriptive(cand):
                return cand
    m = RE_NAME_DASH.match(h)
    if m:
        cand = m.group(1).strip()
        if not looks_descriptive(cand):
            return cand
    return None


def extract_age(headline, abstract):
    for txt in (headline, abstract):
        if not txt: continue
        m = RE_AGE_DIES.search(txt)
        if m: return int(m.group(1))
    if headline:
        m = RE_AGE_COMMA_HEAD.search(headline)
        if m and 18 <= int(m.group(1)) <= 120: return int(m.group(1))
    if abstract:
        # "Mr. Gugino, 81, had filed…" — age follows comma in abstract
        m = RE_AGE_COMMA_HEAD.search(abstract)
        if m and 18 <= int(m.group(1)) <= 120: return int(m.group(1))
    if abstract:
        m = RE_AGE_WAS.search(abstract)
        if m and 18 <= int(m.group(1)) <= 120: return int(m.group(1))
    return None


def extract_gender(name, full_text):
    """Return (gender, source) where source is one of:
        'honorific' — Mr./Mrs./Sir/Dame/Lord/etc. in text (highest confidence)
        'pronoun'   — he/she/his/her tally in text (high confidence)
        'first_name' — match against US baby-name lists (lowest confidence;
                       culture-specific and unreliable for non-Western names)
        None         — could not determine
    """
    if full_text:
        t = ' ' + full_text + ' '  # case-sensitive — honorifics are capitalized
        # Mx. — non-binary honorific (rarely used in NYT obits, but unambiguous
        # when present). Mx. + capitalized name token. Beats Mr./Mrs./Ms. since
        # those don't apply.
        x_strong = len(re.findall(r'\bMx\.?\s+[A-Z]', t))
        if x_strong: return ('X', 'honorific')
        # Strong honorific: title immediately followed by a capitalized name token.
        # More reliable than bare "Dame" (which appears in stage names like Dame
        # Edna) or bare "Sir/Lord" (titles for someone other than the subject).
        m_strong = len(re.findall(r'\bMr\.?\s+[A-Z]', t))
        f_strong = (len(re.findall(r'\bMrs\.?\s+[A-Z]', t))
                    + len(re.findall(r'\bMs\.?\s+[A-Z]', t)))
        if m_strong and m_strong > f_strong: return ('M', 'honorific')
        if f_strong and f_strong > m_strong: return ('F', 'honorific')
        m_hon = (m_strong + len(re.findall(r'\bSir\s', t))
                 + len(re.findall(r'\b(?:Lord|Baron|Count|Duke|Prince|King|Emperor|'
                                  r'Rabbi|Bishop|Cardinal|Pope|Father|Brother|Friar|'
                                  r'Reverend|Monsignor|Imam|Sheikh|Sheik|Sultan|Tsar|'
                                  r'Czar|Maharaja|Shah|Patriarch)\s', t)))
        f_hon = (f_strong + len(re.findall(r'\b(?:Dame|Lady|Baroness|Countess|Duchess|'
                                          r'Princess|Queen|Empress|Madame|Madam|'
                                          r'Sister|Nun|Abbess|Sultana)\s', t)))
        if m_hon and not f_hon: return ('M', 'honorific')
        if f_hon and not m_hon: return ('F', 'honorific')
        # Kinship signals — strong gender indicators when the subject is
        # described AS a relation, or HAS a relation that pins their gender.
        # "the father of …" / "his wife" / "widower" → male
        # "the mother of …" / "her husband" / "widow" → female
        # We require the relation to plausibly refer to the subject; the
        # patterns are anchored to phrasings characteristic of obit prose.
        tl_full = t.lower()
        m_kin = (
            len(re.findall(r'\b(?:was|is)\s+(?:a|the)\s+father\s+of\b', tl_full))
            + len(re.findall(r'\b(?:the|a)\s+father\s+of\b', tl_full))
            + len(re.findall(r'\bwidower\b', tl_full))
            + len(re.findall(r'\bhis\s+(?:wife|widow|son|daughter|children|brother|sister)\b', tl_full))
            + len(re.findall(r'\bsurvived\s+by\s+his\b', tl_full))
        )
        f_kin = (
            len(re.findall(r'\b(?:was|is)\s+(?:a|the)\s+mother\s+of\b', tl_full))
            + len(re.findall(r'\b(?:the|a)\s+mother\s+of\b', tl_full))
            + len(re.findall(r'\bher\s+(?:husband|widower|son|daughter|children|brother|sister)\b', tl_full))
            + len(re.findall(r'\bsurvived\s+by\s+her\b', tl_full))
            # "widow" is risky — "his widow" describes a woman survivor of a
            # male subject; "X is the widow of Y" describes the female subject.
            # Only count "widow" when not preceded by "his" / "left a" / "his late".
            + len(re.findall(r'(?<!his\s)(?<!left\s)(?<!a\s)(?<!the\s)\bwidow\s+of\b', tl_full))
        )
        if m_kin and not f_kin: return ('M', 'kinship')
        if f_kin and not m_kin: return ('F', 'kinship')
        if m_kin and m_kin > f_kin * 1.5: return ('M', 'kinship')
        if f_kin and f_kin > m_kin * 1.5: return ('F', 'kinship')
        tl = tl_full
        he = tl.count(' he ') + tl.count(' his ') + tl.count(' him ')
        she = tl.count(' she ') + tl.count(' her ') + tl.count(' herself ')
        # Single-direction signal accepted at 1+ (catches short abstracts)
        if he >= 1 and she == 0: return ('M', 'pronoun')
        if she >= 1 and he == 0: return ('F', 'pronoun')
        # Both directions: require margin
        if he >= 2 and he > she * 1.5: return ('M', 'pronoun')
        if she >= 2 and she > he * 1.5: return ('F', 'pronoun')
    # First-name fallback (US baby-name lists; flagged as low-confidence)
    if name:
        first = name.split()[0].rstrip(',.').strip()
        first = re.sub(r'[\u2018\u2019\u201C\u201D\'"]', '', first)
        if first in MALE_NAMES: return ('M', 'first_name')
        if first in FEMALE_NAMES: return ('F', 'first_name')
    return (None, None)


_RE_AGE_PART = re.compile(r'^\d{2,3}$')
_RE_DIES_SEMI = re.compile(
    r'\b(?:dies?\s+at\s+\d+|is\s+dead\s+at\s+\d+|dead\s+at\s+\d+|dies?|is\s+dead)\s*;\s*(.+)',
    re.I,
)


def _clean_role(raw):
    role = raw.strip()
    role = re.sub(r'\s+(?:who|that|whose|which)\b.*$', '', role, flags=re.I)
    role = re.sub(r'\b(?:is\s+|was\s+)?(?:dies|dead|died|is\s+dead)\b.*$', '', role, flags=re.I).strip()
    role = re.sub(r'^(?:who|that|whose|of|with|by)\s+', '', role, flags=re.I).strip()
    role = clean_smart_quotes(role)
    role = role.rstrip('.,;:').strip()
    role = re.sub(r'^(?:[Aa]n?|[Tt]he)\s+', '', role)
    if not role or role.isdigit(): return None
    if len(role) < 3 or len(role) > 80: return None
    return role


def extract_profession(headline):
    if not headline: return None
    h = re.sub(r'^Overlooked No More:\s*', '', headline, flags=re.I)
    h = re.sub(r'^OBITUARY\s*:\s*', '', h, flags=re.I)
    h = re.sub(r'^[\u2018\u201C\'"][^\u2019\u201D\'"]+[\u2019\u201D\'"]\s*[:,]\s*', '', h)
    parts = h.split(',')
    if len(parts) < 2: return None

    # Primary pattern: Name, Profession[, Dies at…]
    if not _RE_AGE_PART.match(parts[1].strip()):
        role = _clean_role(parts[1])
        if role: return role

    # Secondary pattern: Name, Age, Profession  (age in parts[1])
    if len(parts) >= 3 and _RE_AGE_PART.match(parts[1].strip()):
        role = _clean_role(','.join(parts[2:]))
        if role: return role

    # Tertiary: Name Dies at N; Short description
    m = _RE_DIES_SEMI.search(h)
    if m:
        role = _clean_role(m.group(1))
        if role: return role

    return None
def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, '*.json')))
    print(f"Scanning {len(files)} monthly raw files...")

    all_obits = []
    by_year = Counter()
    skipped_corr = 0
    skipped_non_obit = 0
    # (year, normalized-headline) → (h, ab, snip, lead) for republication lookup.
    # Republished obits ("From YYYY: …") often have an empty lead and an
    # abstract rewritten without age. The original article in YYYY raw dump
    # usually carries the age in headline ("Dies at 86") or lead ("She was 86").
    src_index = {}

    def _norm_headline(h):
        if not h: return ''
        s = RE_FROM_YEAR.sub('', h).lower()
        s = re.sub(r'[\u2018\u2019\u201C\u201D"\']+', '', s)
        s = re.sub(r'[^\w\s]', ' ', s)
        return re.sub(r'\s+', ' ', s).strip()

    # Death-marker patterns that confirm a single-subject obit even when the
    # article was caught only via news_desk=Obits / section=Obituaries.
    RE_DEATH_HEADLINE = re.compile(
        r'\b(?:Dies?\s+at\s+\d|Is\s+Dead\s+at\s+\d|Dead\s+at\s+\d|Dies?|Is\s+Dead|'
        r'Dead\b|,\s*\d{2,3}\s*,)',
        re.I,
    )
    # Strong URL pattern — explicit "obituary" or "/obituaries/" in slug.
    # Promotes an article to is_obit even when section/tom don't tag it
    # (e.g. Pope John Paul II 2005, filed under section=World with URL
    # /obituary-karol-wojtyla-…). Limited to obituary-shaped slugs to
    # avoid sweeping in unrelated crime/war coverage.
    RE_OBIT_URL_STRONG = re.compile(r'/obituar(?:y|ies)[/\-]', re.I)
    # Loose hint — used inside the looks_like_obit gate to confirm an
    # already-tagged article (section=Obituaries, tom=Obit, etc.) really
    # is one. Includes 'killed' for war/violence-cause obits (Qaddafi
    # 2011, headline lacks any death verb).
    RE_OBIT_URL_HINT = re.compile(r'-(?:dead|dies|obituary|killed)\b|/obituar(?:y|ies)[/\-]', re.I)

    for f in files:
        with open(f) as fh:
            try:
                docs = json.load(fh)
            except Exception:
                continue
        for d in docs:
            tom = d.get('type_of_material', '') or ''
            news_desk = d.get('news_desk', '') or ''
            section = d.get('section_name', '') or ''
            url = d.get('web_url', '')
            if url.startswith('https://www.nytimes.com'):
                url = url.replace('https://www.nytimes.com', '')
            # Skip corrections — they have a dedicated Corrections tab and
            # aren't actual obituaries. Common in 2008-2010 ("For The Record"
            # daily obit corrections column).
            if tom == 'Correction':
                skipped_corr += 1
                continue
            # Identify obits — accept either type tag or Obits desk OR Obituaries section.
            # Also include the 9/11 "Portraits of Grief" series whose desk is
            # "National / Portraits of Grief" and whose headlines have no death verb.
            is_portraits = bool(
                RE_PORTRAITS_DESK.search(news_desk)
                or RE_PORTRAITS_URL.search(url)
            )
            is_obit = (tom == 'Obituary (Obit)'
                       or tom == 'Obituary'
                       or tom == 'Obituary; Biography'
                       or tom == 'Biography; Obituary'
                       or news_desk == 'Obits'
                       or section == 'Obituaries'
                       or is_portraits
                       or RE_OBIT_URL_STRONG.search(url))
            if not is_obit:
                continue
            # Drop slideshows and videos — many sit on the Obits desk but carry
            # no parseable subject in the headline.
            if tom in ('Slideshow', 'Video', 'Audio'):
                skipped_non_obit += 1
                continue

            h = d.get('headline', {}).get('main', '') or ''

            # Hard rejects: year-end roundups, lesson plans, multi-subject packages,
            # slideshows/videos by URL pattern
            if RE_NON_OBIT_URL.search(url) or RE_GROUP_HEADLINE.match(h) or url in NON_OBIT_URLS:
                skipped_non_obit += 1
                continue
            # Even when tom='Obituary (Obit)' the article may be an essay-style
            # appreciation that lacks a name in the headline (Pepe Mujica,
            # Jimmy Cliff). Require a death-marker, obit-style URL slug, or a
            # known series prefix — otherwise drop. This catches both:
            #   - desk/section-only obits that aren't really obits (related
            #     articles riding the Obits desk byline)
            #   - tom-tagged appreciation pieces with no name in the headline
            # Portraits of Grief bypass this gate — they're profile-style.
            # Essay-style obits with no death verb — bypass looks_like_obit for known URLs.
            ESSAY_OBIT_URLS = {
                '/2005/04/03/world/europe/allembracing-man-of-action-for-a-new-era-of-papacy.html',  # Pope John Paul II
                '/2005/04/01/international/europe/allembracing-man-of-action-for-a-new-era-of-papacy.html',
                '/2005/04/02/international/europe/allembracing-man-of-action-for-a-new-era-of-papacy.html',
                '/2011/03/27/us/politics/27geraldine-ferraro.html',   # Geraldine Ferraro
                '/2010/12/14/world/14holbrooke.html',                  # Richard Holbrooke
                '/2010/12/08/us/08edwards.html',                       # Elizabeth Edwards
                '/2004/03/09/nyregion/body-of-spalding-gray-found-monologuist-and-actor-was-62.html',  # Spalding Gray
            }
            looks_like_obit = bool(
                is_portraits
                or url in ESSAY_OBIT_URLS
                or RE_DEATH_HEADLINE.search(h)
                or RE_OBIT_URL_HINT.search(url)
                or RE_LEADING_SERIES.match(h)
                or RE_FROM_YEAR.match(h)
            )
            if not looks_like_obit:
                skipped_non_obit += 1
                continue

            ab = d.get('abstract', '') or ''
            snip = d.get('snippet', '') or ''
            lead = d.get('lead_paragraph', '') or ''
            # For republished obits, strip the boilerplate "This obituary was
            # originally published…" sentence so gender/age detection sees the
            # actual obit prose underneath.
            # "Not Forgotten:" interactives (e.g. /interactive/2016/.../aaliyah.html)
            # are retrospective republications of an existing obit and should
            # merge onto the original via the cross-year same-name pass.
            is_not_forgotten = bool(re.match(r'^\s*Not Forgotten\s*[:\u2014\u2013-]', h, re.I))
            republished = bool(RE_FROM_YEAR.match(h)) or is_not_forgotten or bool(
                re.search(r'(?:originally\s+published|being\s+republished)', lead, re.I)
            )
            lead_clean = RE_REPUB_BOILER.sub('', lead) if republished else lead
            # Include the headline in gender-extraction text — captures pronouns
            # and honorifics from headlines like "A Guy Just Like Him".
            full = ' '.join([h, ab, snip, lead_clean])

            name = extract_name(h, url, is_portraits=is_portraits)
            # Abstract fallback: many essay-style obits have "Full Name, 75,
            # verb..." in the abstract even when the headline is descriptive.
            # Also runs when headline returned only a single token (suspicious
            # — may be a role word the parser didn't filter).
            name_from_abstract = extract_name_from_abstract(ab or snip or lead_clean)
            if not name or (name_from_abstract and ' ' in name_from_abstract and ' ' not in name):
                name = name_from_abstract or name
            # Slug-based fallback: URL slug often contains the name when both
            # headline and abstract parsing fail.
            if not name:
                name = extract_name_from_slug(url)
            age = extract_age(h, ab + ' ' + snip + ' ' + lead_clean)
            # Portraits of Grief headlines are "Name: Tagline" — the tagline is
            # an evocative one-liner ("A Cousin's Funny Antics"), not a role.
            # Skip profession parsing rather than emit garbage.
            prof = None if is_portraits else extract_profession(h)
            gen, gen_src = extract_gender(name, full)
            # Overlooked No More: detect from headline prefix, but also from
            # URL slug — some entries (Margaret Garner) have a slug ending in
            # -overlooked.html with no headline prefix.
            overlooked = bool(
                re.match(r'^Overlooked No More\b', h, re.I)
                or re.search(r'/overlooked-|-overlooked(?:\.html|/|$)', url, re.I)
            )

            pub = d.get('pub_date', '')[:10]
            year = pub[:4] if pub else ''

            display_name = make_display_name(h, name) if name else name
            wc = d.get('word_count', 0) or 0
            try:
                wc = int(wc)
            except (TypeError, ValueError):
                wc = 0
            all_obits.append({
                'name': name,
                'display_name': display_name,
                'age': age,
                'gender': gen,
                'gender_src': gen_src,
                'profession': prof,
                'overlooked': overlooked,
                'republished': republished,
                'portraits': is_portraits,
                'date': pub,
                'year': year,
                'section': section,
                'desk': news_desk,
                'tom': tom,
                'headline': h,
                'abstract': ab,
                'url': url,
                'word_count': wc,
            })
            by_year[year] += 1

            # Index every (non-republished) obit so republications can look it up.
            if year and not republished:
                norm = _norm_headline(h)
                if norm:
                    src_index[(year, norm)] = (h, ab, snip, lead)

    # Corpus-driven first-name → gender lookup. Build a map from first names
    # we've classified with high confidence (honorific or pronoun) to their
    # majority gender, then apply it to records still missing gender. The
    # built-in SSA baby-name lists are tiny (162 / 254 names); leveraging the
    # ~28k records we *did* classify catches names like Bob, Lucien, Salvatore,
    # Naftali, Keizo that the SSA lists miss.
    from collections import Counter as _Counter, defaultdict as _DD
    _name_g = _DD(_Counter)
    for o in all_obits:
        g = o.get('gender')
        src = o.get('gender_src')
        # Only learn from high-confidence sources
        if not g or src == 'first_name': continue
        nm = o.get('name') or ''
        parts = nm.split()
        if not parts: continue
        first = re.sub(r'[\u2018\u2019\u201C\u201D\'"]', '',
                       parts[0].rstrip(',.').strip())
        if len(first) < 2: continue
        _name_g[first][g] += 1
    # Names where one gender dominates (≥3 obits, ≥90% one direction)
    _corpus_M = set()
    _corpus_F = set()
    for nm, c in _name_g.items():
        total = sum(c.values())
        if total < 3: continue
        m = c.get('M', 0); f = c.get('F', 0)
        if m >= 3 and m / total >= 0.90: _corpus_M.add(nm)
        elif f >= 3 and f / total >= 0.90: _corpus_F.add(nm)
    n_corpus_recovered = 0
    for o in all_obits:
        if o.get('gender'): continue
        nm = o.get('name') or ''
        parts = nm.split()
        if not parts: continue
        first = re.sub(r'[\u2018\u2019\u201C\u201D\'"]', '',
                       parts[0].rstrip(',.').strip())
        if first in _corpus_M:
            o['gender'] = 'M'; o['gender_src'] = 'corpus_first_name'
            n_corpus_recovered += 1
        elif first in _corpus_F:
            o['gender'] = 'F'; o['gender_src'] = 'corpus_first_name'
            n_corpus_recovered += 1
    print(f"Gender recovered via corpus first-name lookup ({len(_corpus_M)} M, "
          f"{len(_corpus_F)} F names): {n_corpus_recovered}")

    # Republished-obit age recovery — when the API's republication record has
    # no age (lead is empty, abstract is editorially rewritten), look up the
    # original article in YYYY's raw dump and extract age there. Republished
    # headlines are often a *prefix* of the original (e.g. "Hedy Lamarr,
    # Sultry Star Who Reigned in Hollywood" vs the original "…in Hollywood
    # Of 30's and 40's, Dies at 86"), so we fall back to a startswith match
    # within the source year and ±1 neighbors.
    n_repub_age_recovered = 0
    _RE_FROM_YR_CAP = re.compile(r'^(?:From\s*[:\u2014\u2013-]?\s*)?From\s+(\d{4})', re.I)
    src_by_year = {}
    for (yr, nh), rec in src_index.items():
        src_by_year.setdefault(yr, []).append((nh, rec))

    def _find_orig(src_year, repub_norm):
        if not repub_norm or len(repub_norm.split()) < 3:
            return None
        rec = src_index.get((src_year, repub_norm))
        if rec: return rec
        for adj in (0, -1, 1):
            yr = str(int(src_year) + adj)
            for nh, rec in src_by_year.get(yr, []):
                if nh.startswith(repub_norm) or repub_norm.startswith(nh):
                    return rec
        return None

    for o in all_obits:
        if not o.get('republished') or o.get('age'):
            continue
        m_yr = _RE_FROM_YR_CAP.match(o.get('headline') or '')
        if not m_yr:
            continue
        src_year = m_yr.group(1)
        norm = _norm_headline(o.get('headline') or '')
        rec = _find_orig(src_year, norm)
        if not rec:
            continue
        h2, ab2, sn2, lp2 = rec
        age2 = extract_age(h2, (ab2 or '') + ' ' + (sn2 or '') + ' ' + (lp2 or ''))
        if age2:
            o['age'] = age2
            n_repub_age_recovered += 1
    print(f"Republished-obit ages recovered via cross-year lookup: {n_repub_age_recovered}")

    # Per-URL overrides for cases the parser can't get right programmatically.
    n_overrides = 0
    for o in all_obits:
        ov = OBIT_OVERRIDES.get(o.get('url') or '')
        if not ov: continue
        for k, v in ov.items():
            o[k] = v
        # If the override touched name, regenerate display_name so it tracks.
        if 'name' in ov:
            o['display_name'] = ov.get('display_name') or ov['name']
        n_overrides += 1
    print(f"Per-URL overrides applied: {n_overrides}")

    # Multi-subject splits: emit one record per named subject from a single
    # article (e.g. /2003/01/17/.../2-archaeologists-robert-braidwood-95-and-
    # his-wife-linda-braidwood-93-die.html → two records).
    n_split_in = 0
    n_split_out = 0
    new_obits = []
    for o in all_obits:
        splits = OBIT_SPLITS.get(o.get('url') or '')
        if not splits:
            new_obits.append(o)
            continue
        n_split_in += 1
        for s in splits:
            r = dict(o)
            for k, v in s.items():
                r[k] = v
            r['display_name'] = s.get('display_name') or s.get('name') or r.get('name')
            new_obits.append(r)
            n_split_out += 1
    if n_split_in:
        print(f"Multi-subject splits: {n_split_in} URLs → {n_split_out} records")
    all_obits = new_obits

    # Merge same-name records published within ±10 days. The Times often runs
    # an initial obit and a follow-up profile within a week (Peter Gowland:
    # 2010-04-01 and 2010-04-05). Keep the canonical entry as primary, but
    # surface the secondary URL so the reader can see it.
    from datetime import date as _date
    # 'Obituary; Biography' / 'Biography; Obituary' tag the canonical long-form
    # NYT obit; plain 'Obituary' is often used for short stubs (Rehnquist 2005:
    # 93-word teaser tagged 'Obituary' next to the 6,270-word real obit tagged
    # 'Obituary; Biography'). Rank the biography combos above plain Obituary.
    _PREF = {'Obituary (Obit)': 0, 'Obituary; Biography': 1,
             'Biography; Obituary': 1, 'Obituary': 2}
    def _rank(o):
        # Lower = preferred. word_count first so we always pick the longest
        # canonical obit within a same-name cluster, regardless of how each
        # variant is tagged (Althea Gibson 2003: 1,991-word 'News' obit beats
        # 69-word 'Obituary; Biography' stub; Byron White 2002: 4,314-word
        # 'News' beats 3,881-word 'Obituary; Biography'). tom rank only as
        # tiebreaker when word counts are equal.
        try:
            wc = int(o.get('word_count') or 0)
        except (TypeError, ValueError):
            wc = 0
        return (-wc, _PREF.get(o.get('tom') or '', 9),
                -len(o.get('headline') or ''),
                -len(o.get('url') or ''))
    def _parse_date(s):
        try: return _date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        except Exception: return None

    # Group by *normalized* name so middle-initial variants merge
    # (e.g. "Theodore Kupferman" + "Theodore R. Kupferman" 2003-09-20/21).
    def _norm_name(name):
        if not name: return ''
        n = unicodedata.normalize('NFKD', name)
        n = ''.join(c for c in n if not unicodedata.combining(c))
        n = re.sub(r'^(?:Mr|Mrs|Ms|Mx|Dr|Prof|Sir|Lord|Lady|Cardinal|Bishop|Rev|Sister|Father|Brother|Sen|Rep|Gov|Pres|Capt|Col|Gen|Maj|Lt|Sgt|Hon)\.?\s+', '', n, flags=re.I)
        n = re.sub(r',?\s+(?:Jr|Sr|II|III|IV|V|Esq|MD|PhD|DDS)\.?\s*$', '', n, flags=re.I)
        # Insert space between consecutive initial-period tokens so
        # "R.W." normalizes the same way as "R. W." (both → "R W"
        # → drop both as 1-letter tokens). Without this, "R.W. Apple"
        # → "rw apple" while "R. W. Apple Jr." → "apple", and they
        # don't cluster.
        n = re.sub(r'([A-Z])\.([A-Z])', r'\1. \2', n)
        n = re.sub(r"['\u2018\u2019.\-]", '', n)
        n = re.sub(r'\s+', ' ', n).strip().lower()
        toks = [t for t in n.split() if len(t) > 1]   # drop single-letter middle inits
        return ' '.join(toks)

    by_name = {}
    no_name_rows = []
    for o in all_obits:
        if not o.get('name'):
            no_name_rows.append(o)
            continue
        by_name.setdefault(_norm_name(o['name']) or o['name'], []).append(o)

    merged = []
    n_merged = 0
    for name, recs in by_name.items():
        recs_sorted = sorted(recs, key=lambda o: o.get('date') or '')
        clusters = []
        for r in recs_sorted:
            d = _parse_date(r.get('date') or '')
            if not clusters:
                clusters.append([r])
                continue
            last = clusters[-1][-1]
            d2 = _parse_date(last.get('date') or '')
            if d and d2 and abs((d - d2).days) <= 10:
                clusters[-1].append(r)
            else:
                clusters.append([r])
        for cluster in clusters:
            if len(cluster) == 1:
                merged.append(cluster[0])
            else:
                # Pick canonical primary, attach secondary URLs from the rest
                primary = sorted(cluster, key=_rank)[0]
                others = [c for c in cluster if c is not primary]
                primary['secondary_urls'] = [c.get('url') for c in others if c.get('url')]
                primary['secondary_dates'] = [c.get('date') for c in others if c.get('date')]
                merged.append(primary)
                n_merged += len(others)
    merged.extend(no_name_rows)
    # Second-pass merge — republications years apart from their originals
    # (Ruth Clement Bond: original 2005, republication 2026). The ±10-day
    # cluster above can't catch them. For each republished obit with the
    # same name as a non-republished obit, attach the republication as
    # additional URL on the original and drop the republication record.
    by_name2 = {}
    for o in merged:
        nm = o.get('name')
        if nm: by_name2.setdefault(_norm_name(nm) or nm, []).append(o)
    n_repub_merged = 0
    drop_ids = set()
    for nm, recs in by_name2.items():
        if len(recs) < 2: continue
        repubs = [r for r in recs if r.get('republished')]
        origs  = [r for r in recs if not r.get('republished')]
        # Case A: at least one orig + ≥1 repub — attach repubs to canonical orig.
        # Case B: no orig but ≥2 repubs (subject died pre-2000, e.g. Judy
        # Garland) — collapse to the oldest republication.
        if origs and repubs:
            primary = sorted(origs, key=_rank)[0]
            others = repubs
        elif len(repubs) >= 2 and not origs:
            repubs_sorted = sorted(repubs, key=lambda r: r.get('date') or '')
            primary = repubs_sorted[0]
            others = repubs_sorted[1:]
        else:
            continue
        sec_urls = list(primary.get('secondary_urls') or [])
        sec_dates = list(primary.get('secondary_dates') or [])
        for r in others:
            u = r.get('url'); d = r.get('date')
            if u: sec_urls.append(u)
            if d: sec_dates.append(d)
            drop_ids.add(id(r))
            n_repub_merged += 1
        primary['secondary_urls'] = sec_urls
        primary['secondary_dates'] = sec_dates
    merged = [o for o in merged if id(o) not in drop_ids]
    n_total_before = len(all_obits)
    print(f"Skipped {skipped_corr:,} correction-notice records (paper-wide tom='Correction', "
          f"used by Corrections tab; not corrections to obits), {skipped_non_obit:,} non-obit "
          f"package/lesson articles; merged {n_merged:,} same-name near-duplicates "
          f"(±10 days), {n_repub_merged:,} republications onto their originals. "
          f"{n_total_before:,} → {len(merged):,}")
    all_obits = merged
    n_repub = sum(1 for o in all_obits if o.get('republished'))
    n_overl = sum(1 for o in all_obits if o.get('overlooked'))
    n_port = sum(1 for o in all_obits if o.get('portraits'))
    print(f"  republished: {n_repub:,}   overlooked-no-more: {n_overl:,}   "
          f"portraits-of-grief: {n_port:,}")

    print(f"\nTotal obits: {len(all_obits):,}")
    print("By year:")
    for y in sorted(by_year):
        print(f"  {y}: {by_year[y]:,}")

    # Coverage stats
    n = len(all_obits)
    n_name = sum(1 for o in all_obits if o['name'])
    n_age = sum(1 for o in all_obits if o['age'])
    n_gen = sum(1 for o in all_obits if o['gender'])
    n_prof = sum(1 for o in all_obits if o['profession'])
    print(f"\nCoverage of n={n:,}:")
    print(f"  name:       {n_name:,} ({100*n_name/n:.0f}%)")
    print(f"  age:        {n_age:,} ({100*n_age/n:.0f}%)")
    print(f"  gender:     {n_gen:,} ({100*n_gen/n:.0f}%)")
    print(f"  profession: {n_prof:,} ({100*n_prof/n:.0f}%)")
    print(f"\n  gender breakdown: M={sum(1 for o in all_obits if o['gender']=='M'):,} "
          f"F={sum(1 for o in all_obits if o['gender']=='F'):,}")
    src_count = Counter(o.get('gender_src') for o in all_obits if o.get('gender'))
    print(f"  gender by source:")
    for s in ('honorific', 'pronoun', 'first_name'):
        n = src_count.get(s, 0)
        pct = 100 * n / max(1, sum(src_count.values()))
        print(f"    {s:12s} {n:>6,}  ({pct:.1f}%)")

    with open(OUT_PATH, 'w') as f:
        json.dump(all_obits, f, separators=(',', ':'))
    print(f"\nSaved {OUT_PATH} ({os.path.getsize(OUT_PATH):,} bytes)")


if __name__ == '__main__':
    main()
