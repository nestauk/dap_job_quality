"""
Custom prodigy recipe to use the NER model to pre-label benefits,
add additional NER labels and allow for free text entry.

To run the custom recipe (in the prodigy directory):

prodigy benefits_classification job_quality_annotated \
    ./labelled_data/20240117_ads_to_label_ts_1000_random_seed_42.jsonl \
    -F custom_recipe.py
"""
from dap_job_quality import PROJECT_DIR, logger

from typing import Iterator
from pathlib import Path

import prodigy
import spacy
import copy

from prodigy.components.loaders import JSONL
from prodigy.components.preprocess import add_tokens

# LOAD NER MODEL
model_folder = PROJECT_DIR / "outputs/models/ner_model/20230808"
if not model_folder.exists():
    logger.error(
        f"Model folder {model_folder} does not exist. Please download the model."
    )

nlp = spacy.load(model_folder)


def make_tasks(
    nlp: spacy.language.Language,
    stream: Iterator[dict],
) -> Iterator[dict]:
    """Add predicted entities generated by custom NER model and
        option for free text entry to Prodigy stream.

    Args:
        nlp (spacy.language.Language): spaCy language model

    Yields:
        Iterator[dict]: Iterator of dictionaries with text and spans keys
    """
    for eg in stream:
        doc = nlp(eg["text"])
        spans = []
        task = copy.deepcopy(eg)
        for ent in doc.ents:
            if ent.label_ == "BENEFIT":
                start = ent.start_char
                end = ent.end_char
                token_start = ent.start
                token_end = ent.end
                spans.append(
                    {
                        "start": start,
                        "end": end,
                        "token_start": token_start,
                        "token_end": token_end,
                        "label": str(ent.label_).lower(),
                    }
                )
        task["spans"] = spans

        yield task


@prodigy.recipe(
    "benefits_classification",
    dataset=("The dataset to use", "positional", None, str),
    source=("The source data as a .jsonl file", "positional", None, Path),
)
def custom_ner(dataset, source):
    # Initialize the Prodigy stream
    blocks = [{"view_id": "ner_manual"}, {"view_id": "text_input"}]

    stream = JSONL(source)

    # Add tokens to the stream
    stream = add_tokens(nlp, stream)

    # Add predicted entities and text to the stream
    stream = make_tasks(nlp, stream)

    return {
        "dataset": dataset,  # save annotations in this dataset
        "view_id": "blocks",
        "stream": stream,
        "config": {
            "buttons": ["accept", "reject", "ignore"],
            "labels": [
                # "1_job_type",
                # "1_full_time_part_time",
                # "1_working_hours",
                # "2_pay",
                # "2_entitlements",
                # "3_health_safety_wellbeing",
                # "4_job_design_nature_of_work",
                # "5_social_support_cohesion",
                # "6_voice_representation",
                # "7_work_life_balance",
                "other_benefits",
                # "BENEFIT"
            ],
            "blocks": blocks,
        },
    }
