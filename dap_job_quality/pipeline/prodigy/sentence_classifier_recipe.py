"""
Usage:

prodigy job_ad_sent_cat job_sentences_sample ../../../inputs/labelling/job_sentences.jsonl --labels "company context,role requirement,benefit,other" -F sentence_classifier_recipe.py
"""

import prodigy
from prodigy.components.loaders import JSONL
from prodigy.util import split_string
from prodigy.components.preprocess import add_tokens
import spacy
import random


@prodigy.recipe(
    "job_ad_sent_cat",
    dataset=("The dataset to save annotations to", "positional", None, str),
    source=("Path to the source JSONL file", "positional", None, str),
)
def job_ad_sent_cat(dataset: str, source: str):
    """
    A recipe to classify documents with sentences highlighted, using a dummy model
    for pre-labeling the examples randomly among "context", "requirement", "benefit".

    Parameters:
    dataset: The dataset to save annotations to.
    source: The path to the source JSONL file.
    """

    def get_stream():
        # Load the data from the JSONL file
        for task in JSONL(source):
            full_description = task["clean_description"]
            sentence = task["job_sentences"]
            task_id = task["id"]

            full_description_html = task["clean_description"].replace(
                task["job_sentences"], f"<mark>{task['job_sentences']}</mark>"
            )

            # Prepare the task for Prodigy
            yield {
                "html": f"<div style='white-space: pre-wrap;'>{full_description_html}</div>",
                "options": [
                    {"id": "context", "text": "Context"},
                    {"id": "requirement", "text": "Requirement"},
                    {"id": "benefit", "text": "Benefit"},
                ],
                "meta": {"id": task_id},
            }

    return {
        "dataset": dataset,  # the dataset to save annotations to
        "view_id": "choice",  # use the choice interface
        "stream": get_stream(),  # the stream of tasks
        "config": {
            # Pre-select a random choice for demonstration purposes
            "choice_auto_accept": False,
            "choice_style": "single",  # or "single" if you want only one selection
        },
    }
