"""
Pipeline to extract job quality measures from job adverts
"""
import argparse
from datetime import datetime
import nltk
from nltk import ngrams
from nltk.tokenize import sent_tokenize
import numpy as np
import pandas as pd
import re
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Union, Tuple

from dap_job_quality import logging, BUCKET_NAME
from dap_job_quality.getters.afs_data import get_stratified_sample
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.utils import jobbert
from dap_job_quality.getters.models import (
    sentence_classifier_pca,
    sentence_classifier_lr,
)
from dap_job_quality.getters.keywords import get_keywords

nltk.download("punkt")
nltk.download("stopwords")

sent_model = SentenceTransformer("all-MiniLM-L6-v2")
JOBBERT = "jjzha/jobbert-base-cased"

model = sentence_classifier_lr()
pca = sentence_classifier_pca()

today = datetime.today().strftime("%Y-%m-%d")

JQ_THRESHOLD = 0.3
CS_THRESHOLD = 0.55

LOOKUP = get_keywords()


def extract_job_quality_sentences(
    job_adverts: Union[pd.DataFrame, List[str], str],
    id_col: str = "id",
    text_col: str = "clean_description",
    pca=pca,
    model=model,
    threshold: float = JQ_THRESHOLD,
) -> pd.DataFrame:
    if isinstance(job_adverts, str):
        jobs_df = pd.DataFrame([{id_col: 0, text_col: job_adverts}])
    elif isinstance(job_adverts, list):
        jobs_df = (
            pd.DataFrame(job_adverts, columns=[text_col])
            .reset_index()
            .rename(columns={"index": id_col})
        )
    elif isinstance(job_adverts, pd.DataFrame):
        jobs_df = job_adverts[[id_col, text_col]].copy()
    else:
        raise ValueError("Input must be a DataFrame, a list of strings, or a string")

    jobs_df["sentences"] = jobs_df[text_col].apply(lambda x: sent_tokenize(x))
    jobs_df = jobs_df.explode("sentences")

    ad_embeddings = jobbert.embed_sentences(jobs_df["sentences"].tolist(), JOBBERT, 64)

    X_new_pca = pca.transform(ad_embeddings)

    predictions = model.predict_proba(X_new_pca)[:, 1]

    jobs_df["job_quality_prob"] = predictions

    job_quality_df = jobs_df[jobs_df["job_quality_prob"] >= threshold]

    return job_quality_df.reset_index(drop=True)


def split_ngrams(
    text: str, length: int = 6, n: int = 4
) -> Union[List[Tuple[str, ...]], List[str]]:
    """
    Split the input text into n-grams if the number of words in the text exceeds the specified length.

    Args:
        text (str): The input text to be split into n-grams.
        length (int): The maximum length of text in words before splitting into n-grams. Default is 6.
        n (int): The number of words in each n-gram. Default is 4.

    Returns:
        Union[List[Tuple[str, ...]], List[str]]: A list of n-grams (each n-gram is a tuple of words) if the text
                                                 exceeds the specified length; otherwise, a list containing the
                                                 original text.
    """
    if len(text.split()) > length:
        ngram_list = list(ngrams(text.split(), n))
    else:
        ngram_list = [text]
    return ngram_list


def split_text(text: str) -> List[str]:
    """
    Split the input text by specified delimiters while avoiding splits within numbers or weekdays.

    Eg 'Mon - Fri' should be kept as one string, while '- Salary 30k - Pension 5%' should be split into 'Salary 30k' and 'Pension 5%'.

    Args:
        text (str): The input text to be split.

    Returns:
        List[str]: A list of segments resulting from the split operation.
    """

    # List of weekdays and their abbreviations
    weekdays = [
        "Monday",
        "Mon",
        "Tuesday",
        "Tues",
        "Wednesday",
        "Wed",
        "Thursday",
        "Thu",
        "Friday",
        "Fri",
        "Saturday",
        "Sat",
        "Sunday",
        "Sun",
    ]

    # Regular expression pattern to match weekdays as whole words
    weekdays_pattern = "|".join(map(re.escape, weekdays))

    # Regular expression to split by the delimiters but not within numbers or weekdays
    split_pattern = r"(?<!\d)[,;:\(\)](?!\d)|(?<!\d) - (?!\d)"

    # Split the text using the initial pattern
    initial_splits = re.split(split_pattern, text)

    # Further split to ensure no split happens between weekdays
    final_splits = []
    buffer = ""

    for segment in initial_splits:
        if re.search(rf"\b(?:{weekdays_pattern})\b", buffer) and re.search(
            rf"\b(?:{weekdays_pattern})\b", segment
        ):
            buffer += " - " + segment
        else:
            if buffer:
                final_splits.append(buffer)
            buffer = segment

    if buffer:
        final_splits.append(buffer)

    return final_splits


def extract_ngrams(job_quality_df, col="sentences"):
    # perform further cleaning on the sentences
    job_quality_df["sentences_split"] = job_quality_df[col].apply(split_text)
    job_quality_df_long = job_quality_df.explode("sentences_split")

    # replace digits with 'X'
    job_quality_df_long["sentence_cleaned"] = job_quality_df_long[
        "sentences_split"
    ].str.replace(r"\d", "X", regex=True)

    # get unique ngrams and their counts
    job_quality_df_long["ngrams"] = job_quality_df_long["sentence_cleaned"].apply(
        lambda x: split_ngrams(x, 6, 4)
    )
    job_quality_df_long = job_quality_df_long.explode("ngrams")
    job_quality_df_long["ngrams"] = job_quality_df_long["ngrams"].apply(
        lambda x: " ".join(x) if isinstance(x, tuple) else x
    )

    return job_quality_df_long


def match_to_lookup(ngrams: Union[pd.Series, List[str]], lookup, sent_model, threshold):
    target_phrases = lookup["target_phrase"].tolist()

    target_embeddings = sent_model.encode(target_phrases)

    ngram_embeddings = sent_model.encode(ngrams)

    similarities = cosine_similarity(ngram_embeddings, target_embeddings)

    # Find the index of the highest cosine similarity for each n-gram
    max_indices = np.argmax(similarities, axis=1)

    # Retrieve the corresponding target phrases
    most_similar_phrases = [target_phrases[index] for index in max_indices]

    most_similar_similarities = [
        similarities[i, index] for i, index in enumerate(max_indices)
    ]

    most_similar_pairs = list(
        zip(unique_ngrams, most_similar_phrases, most_similar_similarities)
    )

    matches = pd.DataFrame(
        most_similar_pairs, columns=["ngrams", "target_phrase", "cosine_similarity"]
    )

    return matches[matches["cosine_similarity"] >= threshold]


def match_ngrams_to_adverts(matches, job_quality_df_long):
    job_quality_df_long = pd.merge(
        job_quality_df_long, matches, how="left", left_on="ngrams", right_on="ngrams"
    )

    # Group by 'sentence_cleaned' and 'target_phrase' and find index of max cosine similarity so that you don't get multiple matches
    # to the same target phrase
    idx = job_quality_df_long.groupby(["sentence_cleaned", "target_phrase"])[
        "cosine_similarity"
    ].idxmax()

    # Select the rows with these indices
    jq_df_filtered = job_quality_df_long.loc[idx].reset_index(drop=True)

    return jq_df_filtered.drop(
        ["index", "sentences", "job_quality_prob", "sentence_cleaned"], axis=1
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--production",
        default=False,
        type=bool,
        help="Run the script in production mode or test",
    )

    args = parser.parse_args()
    logging.info(args)

    # Import the job adverts

    if args.production:
        job_adverts = get_stratified_sample()
    else:
        job_adverts = get_stratified_sample().sample(10, random_state=42)

    job_quality_df = extract_job_quality_sentences(
        job_adverts, "id", "clean_description"
    )

    job_quality_df_long = extract_ngrams(job_quality_df)

    ngram_counts = pd.DataFrame(
        job_quality_df_long["ngrams"].value_counts()
    ).reset_index()

    unique_ngrams = ngram_counts["ngrams"]

    matches = match_to_lookup(unique_ngrams, LOOKUP, sent_model, CS_THRESHOLD)

    jq_df_filtered = match_ngrams_to_adverts(matches, job_quality_df_long)

    filename = f"job_quality/early_years/evaluation_sample/job_ads_prod_{args.production}_sample_{len(job_adverts)}_{today}.parquet"
    save_to_s3(BUCKET_NAME, jq_df_filtered, filename)
