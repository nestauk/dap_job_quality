"""
Helper functions for EDA steps that we might want to repeat
"""
from collections import Counter
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
from typing import Union, List, Dict, Tuple
from wordcloud import WordCloud

from dap_job_quality.utils import text_cleaning as tc


def make_wordclouds(
    labelled_spans_df: pd.DataFrame,
    label_categories: Union[List[str], np.ndarray],
    output_dir: str,
) -> None:
    """
    Generate wordclouds for each label category based on the labelled spans dataframe.

    Args:
        labelled_spans_df (pd.DataFrame): A dataframe containing labelled spans.
        label_categories (Union[List[str], np.ndarray]): A list or numpy array of label categories.
        output_dir (str): The directory to save the generated wordclouds.

    Raises:
        ValueError: If there are too many label categories.
    """
    # Create a subplot for each wordcloud
    if len(label_categories) == 1:
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(15, 10))
    elif len(label_categories) == 2:
        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(15, 10))
    elif len(label_categories) == 3:
        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 10))
    elif len(label_categories) == 4:
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 10))
    else:
        raise ValueError("Too many label categories")

    # Flatten the axes array for easy indexing
    axes = axes.flatten()

    for i, label in enumerate(label_categories):
        if i < len(label_categories):  # Ensure we don't go out of bounds
            ax = axes[i]
            text = " ".join(
                labelled_spans_df[labelled_spans_df["label"] == label][
                    "labelled_span"
                ].tolist()
            )
            wordcloud = WordCloud(
                width=800, height=800, background_color="white", min_font_size=10
            ).generate(text)

            ax.imshow(wordcloud, interpolation="bilinear")
            ax.axis("off")
            ax.set_title(label)

    # This will ensure that any extra subplots are not visible
    for j in range(i + 1, len(label_categories)):
        axes[j].axis("off")

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(output_dir)
    plt.show()


def most_common_ngrams(
    df: pd.DataFrame,
    n: int = 2,
    label_col: str = "label",
    text_col: str = "labelled_span",
    n_most_common: int = 10,
) -> Dict[str, List[Tuple[str, int]]]:
    """
    Find the most common n-grams within each category in a dataframe.

    This function assumes you have a dataframe where one column contains a labelled span of text,
    and another column contains the label attributed to that span (eg "job design and nature of work", "other benefit"
    etc). The function iterates over each category,
    tokenizes the text in the corresponding text column, and finds the most common n-grams
    within each category.

    The aim is to find out what phrases come up most frequently in the context of sentences about categories like
    "social support and cohesion" (eg "warm and friendly team", "supportive atmosphere" etc), "job design and
    nature of work" (eg "ongoing training", "career progression" etc).

    Args:
        df (pd.DataFrame): The dataframe containing the text and labels.
        n (int): The number of elements in each n-gram (default is 2).
        label_col (str): The name of the column in the dataframe that contains the category labels (default is 'label').
        text_col (str): The name of the column in the dataframe that contains the text to be tokenized (default is 'labelled_span').
        n_most_common (int): The number of most common n-grams to return for each category (default is 10).

    Returns:
        Dict[str, List[Tuple[str, int]]]: A dictionary where the keys are the categories and the values are lists of tuples.
        Each tuple contains an n-gram and its frequency, and the list is sorted by frequency in descending order.
    """
    category_ngrams = {}
    for category in df[label_col].unique():
        ngrams_list = []
        for text in df[df[label_col] == category][text_col]:
            n_grams = tc.tokenize(text, n)
            ngrams_list.extend(n_grams)
        category_ngrams[category] = Counter(ngrams_list).most_common(n_most_common)
    return category_ngrams


def find_phrase_and_sentence(text: str, phrases: List[str]) -> Tuple[bool, str]:
    """
    Searches for each phrase in a list within the provided text and returns the first sentence containing any phrase.

    #TODO: extend this function so that it can return multiple sentences from the same job ad.

    The function iterates through a list of phrases and checks if any of them are present in the text.
    If a phrase is found, it returns a tuple with True and the sentence containing the phrase.
    If none of the phrases are found in the text, it returns a tuple with False and an empty string.

    Args:
        text (str): The text in which to search for the phrases.
        phrases (List[str]): A list of phrases to search for within the text.

    Returns:
        Tuple[bool, str]: A tuple where the first element is a boolean indicating if any phrase was found,
        and the second element is the sentence containing the phrase. If no phrase is found, the second element is an empty string.
    """
    for phrase in phrases:
        if phrase in text.lower():  # Check if the phrase is in the text
            # Find the whole sentence containing the phrase
            sentence = re.search(
                r"([^.]*?" + re.escape(phrase) + r"[^.]*\.)", text, re.IGNORECASE
            )
            if sentence:
                return True, sentence.group()
    return False, ""
