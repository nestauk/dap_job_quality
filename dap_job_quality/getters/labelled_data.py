from dap_job_quality.getters.data_getters import load_s3_jsonl

from dap_job_quality import BUCKET_NAME, PROJECT_DIR


def get_dummy_job_sentences():
    return load_s3_jsonl(
        BUCKET_NAME,
        s3_file_name="job_quality/sentence_classifier/inputs/labelled/dummy_job_sentences_2024-04-12.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/dummy_job_sentences_2024-04-12.jsonl",
    )
