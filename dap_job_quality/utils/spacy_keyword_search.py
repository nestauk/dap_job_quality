from dap_job_quality.getters.ojo_getters import get_ojo_sample
from dap_job_quality.utils.text_cleaning import clean_text


import pandas as pd
import spacy
import itertools
from spacy.matcher import PhraseMatcher, Matcher
import spacy
from spacy import displacy
from spacy.matcher import Matcher


nlp = spacy.load("en_core_web_sm")
matcher = Matcher(nlp.vocab)


def get_matches(
    text: str,
    patterns: list,
    matcher: Matcher = matcher,
    nlp: spacy.lang.en.English = nlp,
) -> tuple:
    """Takes text and returns spacy matches, based on the patterns.

    Args:
        text (str): A string of text
        patterns (list): a list of spacy matcher patterns, following rule-based patterns: https://spacy.io/usage/rule-based-matching
        matcher (matcher): a spacy matcher object

    Returns:
        _type_: spacy doc and list of matches (tuples of the match id, start span, end span)
    """

    for i in range(0, len(patterns)):
        matcher.add(patterns[i]["subcategory"], patterns[i]["patterns"])

    doc = nlp(text)
    matches = matcher(doc)

    return doc, matches


def get_spans(doc: spacy.tokens.doc.Doc, matches: list, nlp=nlp) -> list:
    """Takes matches and converts them into the format required for the displacy visualiser.
    This also collapses the matches - if two keywords are found for the same label in the same sentence, the
    label will go from the first letter of the first keyword, to the last letter of the last keyword (including all words in between).

    Args:
        matches (list): matches (tuples of the match id, start span, end span)

    Returns:
        list: list of dictionaries, each containing the start and end token, the label, and the sentence. The format for each dictonary is:
        {'start_token': [int: the start token of the match],
        'end_token': [int: the end token of the match],
        'label': [str: a label from the 'subcategory' in the keyword list],
        'sent': [str: the sentence containing the match]}
    """
    # Get a list of individual matches
    spans_list = []
    for i in matches:
        entry = {
            "start_token": i[1],
            "end_token": i[2],
            "label": nlp.vocab.strings[i[0]],
        }
        spans_list.append(entry)

    # Add the respective sentence for each match
    for i in spans_list:
        i["sent"] = doc[i["start_token"]].sent

    # Make a single span per sentence per label
    collapsed_list = []
    for span in spans_list:
        found = False
        for entry in collapsed_list:
            if entry["label"] == span["label"] and entry["sent"] == span["sent"]:
                entry["start_token"] = min(entry["start_token"], span["start_token"])
                entry["end_token"] = max(entry["end_token"], span["end_token"])
                found = True
                break
        if not found:
            collapsed_list.append(span)

    return collapsed_list


def render_spans(doc: spacy.tokens.doc.Doc, matches: list):
    """Renders the full text with labels shown in a jupyter notebook

    Args:
        matches (list): output of the spacy matcher
        doc (spacy Doc): spacy doc object
    """
    spans_list = get_spans(doc, matches)
    test_input = displacy.parse_spans(doc)
    test_input["spans"] = spans_list

    displacy.render(test_input, style="span", manual=True)


def keyword_search_df(
    df: pd.DataFrame,
    patterns: list,
    matcher: Matcher = matcher,
    nlp: spacy.lang.en.English = nlp,
) -> pd.DataFrame:
    """Run the keyword search on a dataframe, and add the spans to the dataframe

    Args:
        df (pd.DataFrame): Dataframe containing ojo job ads (with clean data called 'clean_description')
        patterns (list): the spacy kwyword patterns to search for

    Returns:
        pd.DataFrame: Dataframe with spans added as a column called 'spans'
    """
    df["spans"] = pd.Series()

    for index, row in df.iterrows():
        doc, matches = get_matches(row["clean_description"], patterns, matcher, nlp)
        slist = get_spans(doc, matches, nlp)
        df.at[index, "spans"] = slist
    return df


def get_sentences_on_theme(
    ojo_df: pd.DataFrame, keywords: list, no_records: int = 1000, theme: str = "ALL"
):
    """
    Get  sentences from the given dataframe on a particular theme, using keyword matching.

    Available themese are: ['L&D', 'TOTAL_PACKAGE', 'HOURS', 'CAREER', 'CONTRACT', 'LEAVE',
       'FLEX_LOC', 'PERKS', 'FLEX_HOURS', 'SPONSORSHIP']. The keywords relating to each theme can be found in dap_job_quality.utils.keyword_search_patterns

    Function will take 3 minutes, 30 seconds to run on 1000 records.

    Args:
        ojo_df (pd.DataFrame): DataFrame of ojo sample, containing the 'clean_description' column.
        keywords (list): List of keyword patterns for the labelled sentences.

    Returns:
        list: List of sentences containing the keywords relating to the theme.
    """
    list_of_matches = []

    for i in range(0, no_records):
        doc, matches = get_matches(ojo_df["clean_description"][i], keywords)
        match_dict = {}
        match_dict["id"] = ojo_df["id"][i]
        match_dict["doc"] = doc
        match_dict["matches"] = matches
        list_of_matches.append(match_dict)

    for i in range(len(list_of_matches)):
        spans = get_spans(list_of_matches[i]["doc"], list_of_matches[i]["matches"])
        list_of_matches[i]["spans"] = spans

    match_labels = [i["spans"] for i in list_of_matches]

    filtered_sents = list(itertools.chain.from_iterable(match_labels))

    if not theme == "ALL":
        filtered_sents = [i["sent"] for i in filtered_sents if i["label"] == theme]

    return filtered_sents
