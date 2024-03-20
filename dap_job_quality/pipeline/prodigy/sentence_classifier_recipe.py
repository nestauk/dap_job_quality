"""
Usage:

To start the app:
```
prodigy job_ad_sent_cat job_sentences_sample ../../../inputs/labelling/job_sentences.jsonl -F sentence_classifier_recipe.py
```

To export the labelled data locally:
```
prodigy db-out job_sentences_sample > ../../../inputs/labelled/job_sentences_labelled.jsonl
```

To delete the database:
```
prodigy drop job_sentences_sample
```
"""

import copy
import pandas as pd
from pathlib import Path
import prodigy
from prodigy.components.loaders import JSONL
from prodigy.components.preprocess import add_tokens
import spacy
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
from typing import Iterator

from dap_job_quality import PROJECT_DIR, logger

# LOAD SKILLS NER MODEL
model_folder = PROJECT_DIR / "outputs/models/ner_model/20230808"
if not model_folder.exists():
    logger.error(
        f"Model folder {model_folder} does not exist. Please download the model."
    )

nlp = spacy.load(model_folder)

# LOAD COMPANY DESCRIPTION CLASSIFIER
model = AutoModelForSequenceClassification.from_pretrained(
    "ihk/jobbert-base-cased-compdecs"
)
tokenizer = AutoTokenizer.from_pretrained("ihk/jobbert-base-cased-compdecs")
comp_desc = pipeline("text-classification", model=model, tokenizer=tokenizer)

KEYWORDS = pd.read_csv(PROJECT_DIR / "inputs/keyword_lookup.csv")
KEYWORDS = KEYWORDS["target_phrase"].unique()
KEYWORDS = [keyword.lower() for keyword in KEYWORDS]


def make_span_dict(start, end, token_start, token_end, sent):
    return {
        "start": start,
        "end": end,
        "token_start": token_start,
        "token_end": token_end - 1,
        "text": sent.text,
        "label": "benefit",
    }


def make_tasks(
    nlp: spacy.language.Language, stream: Iterator[dict], keywords=KEYWORDS
) -> Iterator[dict]:
    for eg in stream:
        doc = nlp(eg["text"])
        spans = []
        task = copy.deepcopy(eg)

        comp_descs = []
        skills_and_benefits = []
        for sent in doc.sents:  # Iterate over sentences
            if comp_desc(sent.text)[0]["label"] == "LABEL_1":
                comp_desc_dict = comp_desc(sent.text)[0]
                comp_desc_dict["sentence"] = sent.text
                comp_descs.append(comp_desc_dict)

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

            contains_keyword = any(token.text.lower() in keywords for token in sent)
            contains_skill_entity = any(ent.label_ == "SKILL" for ent in sent.ents)
            is_company_description = comp_desc(sent.text)[0]["label"] == "LABEL_1"

            if (
                contains_keyword
                and not contains_skill_entity
                and not is_company_description
            ):
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
        task["comp_descs"] = comp_descs

        yield task


@prodigy.recipe(
    "job_ad_sent_cat",
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
                "benefit",
            ],
            "blocks": blocks,
        },
    }
