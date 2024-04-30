"""
Usage:

To download data for labelling from S3, navigate to the prodigy directory, and run:

aws s3 cp <input s3 object URL>  ../../../inputs/labelling/<file_name>.jsonl

eg: aws s3 cp s3://open-jobs-lake/job_quality/prodigy/labelling_data/20240418_ads_to_label_ts_500_random_seed_42.jsonl  ../../../inputs/labelling/20240418_ads_to_label_ts_500_random_seed_42.jsonl

To start the app, navigate to this directory and run the following command:
```
prodigy job_ad_sent_cat job_sentences_sample <file input path> -F sentence_classifier_recipe.py

eg segmented sentences (important for EC2): prodigy job_ad_sent_cat job_sentences_sample ../../../inputs/labelling/20240418_ads_to_label_ts_500_random_seed_42.jsonl -F sentence_classifier_recipe.py

eg unsegmented sentences: prodigy job_ad_sent_cat unsegmented_sample labelling_data/20240429_ads_to_label_ts_500_random_seed_42.jsonl -U -F sentence_classifier_recipe.py
```
To export the labelled data locally:
```
prodigy db-out job_sentences_sample > ../../../inputs/labelled/<file_name>.jsonl

eg: prodigy db-out job_sentences_sample > ../../../inputs/labelled/job_sentences_labelled_20240418.jsonl

To save to S3, first export locally, then run:

aws s3 cp   ../../../inputs/labelling/<file_name>.jsonl <output s3 object URL>

eg: aws s3 cp ../../../inputs/labelled/job_sentences_labelled_20240418.jsonl s3://open-jobs-lake/job_quality/prodigy/binary_classifier_labelled_data/20240418/job_sentences_labelled_20240418_local_instance.jsonl

To delete the database:
```
prodigy drop job_sentences_sample
```
"""

import copy
from datetime import datetime
import pandas as pd
from pathlib import Path
import prodigy
from prodigy.components.loaders import JSONL
from prodigy.components.preprocess import add_tokens
import re
import spacy
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
from typing import Iterator

from dap_job_quality import PROJECT_DIR, BUCKET_NAME, logger
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.utils.spacy_keyword_search import get_matches

import ast

# LOAD SKILLS NER MODEL
logger.info("Loading NER model...")
model_folder = PROJECT_DIR / "outputs/models/ner_model/20230808"
if not model_folder.exists():
    logger.error(
        f"Model folder {model_folder} does not exist. Please download the model."
    )

nlp = spacy.load(model_folder)

"""
# LOAD COMPANY DESCRIPTION CLASSIFIER
logger.info("Loading company description classifier...")

model = AutoModelForSequenceClassification.from_pretrained(
    "ihk/jobbert-base-cased-compdecs"
)
tokenizer = AutoTokenizer.from_pretrained("ihk/jobbert-base-cased-compdecs")
comp_desc = pipeline("text-classification", model=model, tokenizer=tokenizer)

logger.info("Models loaded successfully.")
"""

today_date = datetime.today().strftime("%Y-%m-%d").replace("-", "")

OUT_PATH_LOCAL = PROJECT_DIR / "inputs/labelled/job_sentences_labelled.jsonl"
OUT_PATH_LOCAL.parent.mkdir(parents=True, exist_ok=True)
OUT_PATH_S3 = "job_quality/prodigy/binary_classifier_labelled_data/{today_date}/job_sentences_labelled.jsonl"


def make_span_dict(start, end, token_start, token_end, sent):
    return {
        "start": start,
        "end": end,
        "token_start": token_start,
        "token_end": token_end - 1,
        "text": sent.text,
        "label": "benefit",
    }


def make_tasks(nlp: spacy.language.Language, stream: Iterator[dict]) -> Iterator[dict]:
    for eg in stream:
        doc = nlp(eg["text"])
        spans = []
        task = copy.deepcopy(eg)

        skills_and_benefits = []
        for sent in doc.sents:  # Iterate over sentences
            """
            if comp_desc(sent.text)[0]["label"] == "LABEL_1":
                comp_desc_dict = comp_desc(sent.text)[0]
                comp_desc_dict["sentence"] = sent.text
                comp_descs.append(comp_desc_dict)
            """

            skills_and_benefits.append(
                {
                    "sentence": sent.text,
                    "entities": [
                        {
                            "text": ent.text,
                            "start": ent.start_char,
                            "end": ent.end_char,
                            "label": ent.label_,
                        }
                        for ent in sent.ents
                    ],
                }
            )

            start = sent.start_char
            end = sent.end_char
            token_start = sent.start
            token_end = sent.end

            contains_keyword = len(get_matches(sent.text)[1]) > 0
            contains_skill_entity = any(ent.label_ == "SKILL" for ent in sent.ents)

            if contains_keyword and not contains_skill_entity:
                spans.append(make_span_dict(start, end, token_start, token_end, sent))
            else:
                for ent in sent.ents:
                    if ent.label_ == "BENEFIT":
                        spans.append(
                            make_span_dict(start, end, token_start, token_end, sent)
                        )
                        break  # Stop checking this sentence once a 'BENEFIT' entity is found

        # Deduplicate spans based on the sentence text to avoid adding the same sentence more than once
        unique_spans = {span["text"]: span for span in spans}.values()

        task["skills_and_benefits"] = skills_and_benefits
        task["spans"] = list(unique_spans)

        yield task


@prodigy.recipe(
    "job_ad_sent_cat",
    dataset=("The dataset to use", "positional", None, str),
    source=("The source data as a .jsonl file", "positional", None, Path),
    unsegmented=("Don't split sentences", "flag", "U", bool),
)
def custom_ner(dataset, source, unsegmented: bool = False):
    # Initialize the Prodigy stream
    blocks = [{"view_id": "ner_manual"}, {"view_id": "text_input"}]

    stream = JSONL(source)

    if not unsegmented:
        # Custom way to split into chunks of a certain size
        # its not ideal if these are too big (the model struggles)
        # or too small (it's hard to label)
        def split_text(adverts, chunk_size=5):
            for advert in adverts:
                text = advert["text"]
                id = advert["meta"]["job_id"]
                sentences = re.split(r"(?<!\d)\.(?!\d)", text)
                sentences = [
                    sentence.strip()
                    for sentence in sentences
                    if len(sentence.strip()) != 0
                ]
                for sent_id, i in enumerate(range(0, len(sentences), chunk_size)):
                    yield {
                        "text": ". ".join(sentences[i : i + chunk_size]),
                        "meta": {"id": id, "chunk": sent_id},
                    }

        stream = split_text(list(stream))

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
                "benefit",
            ],
            "blocks": blocks,
        },
    }
