import pandas as pd

from dap_job_quality.getters.data_getters import load_s3_jsonl

from dap_job_quality import BUCKET_NAME, PROJECT_DIR


def get_dummy_job_sentences():
    return load_s3_jsonl(
        BUCKET_NAME,
        s3_file_name="job_quality/sentence_classifier/inputs/labelled/dummy_job_sentences_2024-04-12.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/dummy_job_sentences_2024-04-12.jsonl",
    )


def get_labelled_job_sentences():
    """
    Job ads that have been labelled for job quality.

    The output is a list of length 1, and that item is a list of dicts.

    Each dict has the keys:
    'id', 'text', 'tokens', 'skills_and_benefits', 'spans', 'comp_descs', '_input_hash', '_task_hash', '_view_id', 'answer', '_timestamp', '_annotator_id', '_session_id'
    """
    return load_s3_jsonl(
        BUCKET_NAME,
        s3_file_name="job_quality/prodigy/labelled_data/job_sentences_labelled_20240528.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/job_sentences_labelled_20240528.jsonl",
    )


def get_older_labelled_data():
    """The first batch of labelling that we did"""
    return load_s3_jsonl(
        BUCKET_NAME,
        s3_file_name="job_quality/prodigy/binary_classifier_labelled_data/20240416/job_sentences_labelled_20240416.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/job_sentences_labelled_20240416.jsonl",
    )[
        0
    ]  # it's a nested list for some reason


def get_concat_labelled_data():
    """
    Produced by `dap_job_quality/pipeline/sentence_classifier/prep_training_data.py`
    """
    return pd.read_csv(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labellied/train_val_test_20240703.csv"
    )


def get_positive_sents_labelled_for_categories():
    """
    Labelled [here](https://docs.google.com/spreadsheets/d/1bXNmO9vOLG6zdDpHl0Tdw9AeDqb43CrpkXzNGyHRhWI/edit?gid=4769099#gid=4769099)
    """
    return pd.read_csv(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/positive_sents_for_labelling - positive_sents_for_labelling.csv"
    )


def get_additional_examples_underrepresented_categories():
    """Produced by `dap_job_quality/notebooks/pad_out_sample.ipynb` and then manually labelled
    [here](https://docs.google.com/spreadsheets/d/1s4SRbQypfUP0sY88y-NlaOJegHYv21whcBAVudS9Paw/edit?gid=1942213912#gid=1942213912)
    """
    return pd.read_csv(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/additional_green_jobs_examples_20240704_labelled.csv"
    )
