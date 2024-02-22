"""
Helper functions for EDA steps that we might want to repeat
"""

import matplotlib.pyplot as plt
from wordcloud import WordCloud
from typing import Union, List
import pandas as pd
import numpy as np


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
