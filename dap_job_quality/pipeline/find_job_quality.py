"""
Mock up of the pipeline of extracting job quality measures from job adverts
"""

from collections import defaultdict
from nltk import ngrams

import random


def job_quality_sentences(job_adverts):
    """
    Returns a list of the JQ sentences for this job advert
    TODO: use the classifier to do this
    """
    jq_sentences = {}
    for job_id, job_advert in job_adverts.items():
        jq_sentences[job_id] = [
            sentence for sentence in job_advert.split(".") if "quality" in sentence
        ]

    return jq_sentences


def split_words(sentence):
    """
    Split a sentence into words
    TODO: is there a better method?
    """
    return sentence.split(" ")


def get_quality_measures_per_ngram(ngram):
    """
    Return the quality measures for a ngram
    TODO: replace with actual JQ algorithm
    """
    if "annual leave" in ngram:
        return ("annual leave", 0.7)
    elif "flexible hours" in ngram:
        return ("flexible hours", 0.87)
    elif "flexible location" in ngram:
        return ("flexible location", 0.65)
    else:
        return None


def get_quality_measures(ngrams):
    """
    Return the quality measures for a list of ngrams
    TODO: replace with actual JQ algorithm
    """
    ngram_quality = {ngram: get_quality_measures_per_ngram(ngram) for ngram in ngrams}
    ngram_quality = {
        k: v for k, v in ngram_quality.items() if v
    }  # No need to keep the ones that don't map to quality measures
    return ngram_quality


def get_ngrams(jq_sentences_all):

    # Split by sentence, get ngrams and store sentences for later
    job_ad_ngram_dict = defaultdict(list)

    for job_id, jq_sentences in jq_sentences_all.items():
        for sentence in jq_sentences:
            words = split_words(sentence)
            job_ngrams = [" ".join(ng) for ng in list(ngrams(words, 2))]
            job_ad_ngram_dict[job_id].append(job_ngrams)

    # Get all the unique ngrams
    all_ngrams = list(
        set(
            [
                ng
                for sentence_list in job_ad_ngram_dict.values()
                for ng_list in sentence_list
                for ng in ng_list
            ]
        )
    )

    return job_ad_ngram_dict, all_ngrams


def extract_job_quality(job_adverts, jq_sentences_all, job_ad_ngram_dict):

    # Get the results per job advert
    output_dict = {}
    for job_id, _ in job_adverts.items():
        quality_measures_per_sentence = []
        for sentence_ngrams in job_ad_ngram_dict[job_id]:
            quality_measures = [ngram_quality.get(ngram) for ngram in sentence_ngrams]
            quality_measures_per_sentence.append([qm for qm in quality_measures if qm])
        output_dict[job_id] = dict(
            zip(jq_sentences_all[job_id], quality_measures_per_sentence)
        )

    return output_dict


if __name__ == "__main__":

    # Import the job adverts
    job_adverts = {
        0: "This is a job advert. There is a lot of quality measures in it such as flexible location. One quality measure is annual leave.",
        1: "Here is another job advert. The job is for a role in a bank. This is a job quality sentence about flexible location and flexible hours.",
    }

    jq_sentences_all = job_quality_sentences(job_adverts)

    job_ad_ngram_dict, all_ngrams = get_ngrams(jq_sentences_all)

    # Map the unique ngrams to quality measures
    ngram_quality = get_quality_measures(all_ngrams)

    output_dict = extract_job_quality(job_adverts, jq_sentences_all, job_ad_ngram_dict)
    print(output_dict)

    # Outputs the job quality sentences and which JQ measures they map to (with e.g. similarity score)
    # >>> output_dict[0]
    # {' There is a lot of quality measures in it such as flexible location': [('flexible location', 0.65)], ' One quality measure is annual leave': [('annual leave', 0.7)]}
    # >>> output_dict[1]
    # {' This is a job quality sentence about flexible location and flexible hours': [('flexible location', 0.65), ('flexible hours', 0.87)]}
