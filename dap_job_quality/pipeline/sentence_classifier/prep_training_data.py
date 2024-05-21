import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
import spacy

from dap_job_quality import PROJECT_DIR, config, BUCKET_NAME, logging
from dap_job_quality.getters.labelled_data import get_labelled_job_sentences
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.utils import prodigy_data_utils as pdu

nlp = spacy.load("en_core_web_sm")

SEED = config["seed"]


def get_negative_example_sentences(df):
    # Find all unique texts
    unique_texts = df["text"].unique()

    # Split unique texts into sentences
    all_sentences_from_text = set()
    for text in unique_texts:
        doc = nlp(text)
        all_sentences_from_text.update([sent.text for sent in doc.sents])

    # Set of sentences already in the 'sentence' column
    existing_sentences = set(df["sentence"])

    # Find sentences that are not in the 'sentence' column
    new_sentences = all_sentences_from_text - existing_sentences

    return new_sentences


def filter_job_ads(labelled_df):
    job_ids = labelled_df["id"].unique()

    # skip the first 10 job ads - we didn't know what we were labelling at that point
    target_ids = job_ids[10:]

    labelled_df_clean = labelled_df[labelled_df["id"].isin(target_ids)]
    # get rid of empty spans
    labelled_df_clean = labelled_df_clean[labelled_df_clean["span"] != ""]
    return labelled_df_clean


if __name__ == "__main__":
    labelled_data = get_labelled_job_sentences()[
        0
    ]  # all entries are the first item in the list
    labelled_data = pdu.get_spans_and_sentences(labelled_data)

    labelled_df = pd.DataFrame(columns=["span", "sent", "text", "job_id"])

    for key in labelled_data.keys():
        temp_df = pd.DataFrame(labelled_data[key])
        temp_df["id"] = int(key)
        labelled_df = pd.concat([labelled_df, temp_df])

    labelled_df = labelled_df.drop(["job_id"], axis=1)

    labelled_df_clean = filter_job_ads(labelled_df)

    labelled_df_clean["sentence"] = labelled_df_clean["sent"].apply(lambda x: x.text)

    negative_sentences = get_negative_example_sentences(labelled_df_clean)

    negative_df = pd.DataFrame(list(negative_sentences))
    negative_df["label"] = 0
    negative_df.columns = ["sentence", "label"]
    logging.info(negative_df.head())

    positive_df = labelled_df_clean[["sentence"]]
    positive_df["label"] = 1
    logging.info(positive_df.head())

    training_ml_df = pd.concat([positive_df, negative_df])

    # Splitting the dataset into training, validation, and test sets
    X_train, X_temp, y_train, y_temp = train_test_split(
        training_ml_df.drop(["label"], axis=1),
        training_ml_df["label"],
        test_size=0.4,
        random_state=SEED,
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.5, random_state=SEED
    )

    for data, name in zip(
        [X_train, X_val, X_test, y_train, y_val, y_test],
        ["X_train", "X_val", "X_test", "y_train", "y_val", "y_test"],
    ):
        save_to_s3(
            BUCKET_NAME,
            data,
            f"job_quality/sentence_classifier/inputs/labelled/{name}.pkl",
        )
