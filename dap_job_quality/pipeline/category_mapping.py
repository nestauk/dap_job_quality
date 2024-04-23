import json
import numpy as np
import spacy

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

model = SentenceTransformer("all-MiniLM-L6-v2")
nlp = spacy.load("en_core_web_sm")


# Get phrases from the labelled data
def retrieve_phrases(labelled_data: list) -> list:
    """Retrieve phrases from spacy labelled data

    Args:
        labelled_data (list): _description_

    Returns:
        dict:{job id: [phrases labelled as benefits]}
    """
    benefit_list = []

    for entry in labelled_data:
        benefit_dict = {}
        benefit_dict["id"] = entry["id"]
        benefit_dict["phrase"] = []
        doc = nlp(entry["text"])
        for span in entry["spans"]:
            phrase = doc[span["token_start"] : span["token_end"] + 1].text
            benefit_dict["phrase"].append(phrase)
        benefit_list.append(benefit_dict)
    return benefit_list


def calculate_cosine_similarity(text: list, key_phrases: list, model=model) -> float:
    """Calculate the cosine similarity between two phrases, typically the labelled spans from benefit classifier and
    phrases given in the sentences_for_matching dictionary (for each subcategory).

    Args:
        text (list): list of cleaned ojo sentences
        key_phrases (list): phrases to calculate cosine similarity
        model (_type_, optional): _description_. Defaults to SentenceTransformer("all-MiniLM-L6-v2")
        single_phrase (bool, optional): Transforms the vectors if only a singe element is given. Defaults to False.

    Returns:
        float: _description_
    """

    embedding1 = model.encode(text)
    embedding2 = model.encode(key_phrases)

    if len(text) == 1:
        embedding1 = embedding1.reshape(1, -1)
    else:
        pass

    if len(key_phrases) == 1:
        embedding2 = embedding2.reshape(1, -1)
    else:
        pass

    return cosine_similarity(embedding1, embedding2)
