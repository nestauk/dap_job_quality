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
        s3_file_name="job_quality/prodigy/binary_classifier_labelled_data/20240416/job_sentences_labelled_20240416.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/job_sentences_labelled_20240416.jsonl",
    )
