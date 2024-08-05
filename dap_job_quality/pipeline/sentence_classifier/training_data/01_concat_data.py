"""
This script combines batches of data that we've labelled at different points in the project.

Usage:
```
python dap_job_quality/pipeline/sentence_classifier/training_data/01_concat_data.py
```
"""

from nltk.tokenize import sent_tokenize
import pandas as pd
from sentence_transformers import SentenceTransformer

from dap_job_quality import BUCKET_NAME, logging
from dap_job_quality.getters.labelled_data import (
    get_labelled_job_sentences,
    get_older_labelled_data,
)
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.utils import prodigy_data_utils as pdu

model = SentenceTransformer("all-MiniLM-L6-v2")


def match_spans_to_sentences(df):
    for idx, row in df.iterrows():
        span = row["span_sents"]
        if isinstance(span, str):
            for sent in row["sentences"]:
                if (span in sent) | (span == sent):
                    # logging.info(f"'''{span}''' is in {sent}")
                    df.at[idx, "true_sentence"] = sent
        else:
            df.at[idx, "true_sentence"] = None
    return df


if __name__ == "__main__":
    # Concatenate older labelled data (Jan 2024)
    # + the more recent batch that we did in May 2024

    # This is the more recent batch
    labelled_sents = get_labelled_job_sentences()

    final_labelled_ids = set([sent["meta"]["id"] for sent in labelled_sents])

    # This is the earlier data
    early_data = get_older_labelled_data()[10:]
    early_ids = set([record["id"] for record in early_data])

    # check that no data is duplicated
    common_ids = final_labelled_ids.intersection(early_ids)

    if len(common_ids) > 0:
        logging.error(f"Data is duplicated for ids: {common_ids}")
    else:
        logging.info("No duplicated IDs between older and newer data.")

    early_data_spans = pdu.get_spans_and_sentences(early_data, chunks=False)

    early_data_df = pd.DataFrame()

    for id, val in early_data_spans.items():
        temp_df = pd.DataFrame(val)
        temp_df["id"] = id
        early_data_df = pd.concat([early_data_df, temp_df])

    early_data_df = early_data_df.reset_index(drop=True)

    early_data_df["span_sents"] = early_data_df["span"].apply(
        lambda x: sent_tokenize(x)
    )
    early_data_df = early_data_df.explode("span_sents")
    early_data_df["sentences"] = early_data_df["text"].apply(lambda x: sent_tokenize(x))

    early_data_df = early_data_df.reset_index(drop=True)

    early_data_df = match_spans_to_sentences(early_data_df)

    labelled_spans = pdu.get_spans_and_sentences(labelled_sents, chunks=True)

    labelled_spans_df = pd.DataFrame()

    for id, val in labelled_spans.items():
        for chunk, nested_val in val.items():
            temp_df = pd.DataFrame(nested_val)
            temp_df["id"] = id
            temp_df["chunk"] = chunk
            labelled_spans_df = pd.concat([labelled_spans_df, temp_df])

    labelled_spans_df = labelled_spans_df.reset_index(drop=True)

    labelled_spans_df["span_sents"] = labelled_spans_df["span"].apply(
        lambda x: sent_tokenize(x)
    )
    labelled_spans_df = labelled_spans_df.explode("span_sents")
    labelled_spans_df["sentences"] = labelled_spans_df["text"].apply(
        lambda x: sent_tokenize(x)
    )

    labelled_spans_df = match_spans_to_sentences(labelled_spans_df)

    early_data_df = early_data_df[early_data_df["span"] != ""]
    labelled_spans_df = labelled_spans_df[labelled_spans_df["label"] != "none"]

    all_labelled_data = pd.concat(
        [
            early_data_df[
                ["id", "text", "label", "sentences", "span_sents", "true_sentence"]
            ],
            labelled_spans_df[
                ["id", "text", "label", "sentences", "span_sents", "true_sentence"]
            ],
        ]
    ).reset_index(drop=True)

    all_labelled_data["label"] = 1

    all_labelled_data.rename(columns={"true_sentence": "sentence"}, inplace=True)

    # We save the positive sentences and manually label them here:
    # https://docs.google.com/spreadsheets/d/1bXNmO9vOLG6zdDpHl0Tdw9AeDqb43CrpkXzNGyHRhWI/edit?gid=4769099#gid=4769099

    save_to_s3(
        BUCKET_NAME,
        all_labelled_data,
        "job_quality/sentence_classifier/inputs/labelling/positive_sents_for_labelling.csv",
    )
