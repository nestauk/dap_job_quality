from datetime import datetime
from nltk import ngrams
import numpy as np
import pandas as pd
import re
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
from typing import List, Tuple, Union

from dap_job_quality import logging, BUCKET_NAME
from dap_job_quality.getters.afs_data import get_jq_sentences
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.getters.keywords import get_keywords

sent_model = SentenceTransformer("all-MiniLM-L6-v2")

today = datetime.today().strftime("%Y-%m-%d")


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
    text_words = text.split()
    if len(text_words) > length:
        ngram_list = list(ngrams(text_words, n))
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


if __name__ == "__main__":
    # download sample of sentences that we've classified as relating to job quality
    jq_sentences_df = get_jq_sentences()

    # load in the lookup table
    lookup = get_keywords()

    # perform further cleaning on the sentences
    jq_sentences_df["sentences_split"] = jq_sentences_df["sentences"].apply(split_text)
    jq_sentences_df_long = jq_sentences_df.explode("sentences_split")
    logging.info(
        f"Splitting sentences added {len(jq_sentences_df_long) - len(jq_sentences_df)} extra sentences. Total N sentences is now {len(jq_sentences_df_long)}"
    )

    # replace digits with 'X'
    jq_sentences_df_long["sentence_cleaned"] = jq_sentences_df_long[
        "sentences_split"
    ].str.replace(r"\d", "X", regex=True)

    # get unique ngrams and their counts
    jq_sentences_df_long["ngrams"] = jq_sentences_df_long["sentence_cleaned"].apply(
        lambda x: split_ngrams(x, 6, 4)
    )
    jq_sentences_df_long = jq_sentences_df_long.explode("ngrams")
    jq_sentences_df_long["ngrams"] = jq_sentences_df_long["ngrams"].apply(
        lambda x: " ".join(x) if isinstance(x, tuple) else x
    )

    # calculate cosine similarity of unique ngrams to target phrases
    ngram_counts = pd.DataFrame(
        jq_sentences_df_long["ngrams"].value_counts()
    ).reset_index()

    unique_ngrams = ngram_counts["ngrams"]

    target_phrases = lookup["target_phrase"].tolist()

    target_embeddings = sent_model.encode(target_phrases)

    ngram_embeddings = sent_model.encode(unique_ngrams)

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

    matches = pd.merge(matches, ngram_counts, on="ngrams", how="left")

    logging.info(matches["count"].describe())

    jq_sentences_df_long = pd.merge(
        jq_sentences_df_long, matches, how="left", left_on="ngrams", right_on="ngrams"
    )

    save_to_s3(
        BUCKET_NAME,
        jq_sentences_df_long,
        f"job_quality/early_years/jq_sentences/jq_sentences_2023_matched_{today}.parquet",
    )
    save_to_s3(
        BUCKET_NAME,
        matches,
        f"job_quality/early_years/jq_sentences/unique_ngrams_2023_matched_{today}.parquet",
    )
