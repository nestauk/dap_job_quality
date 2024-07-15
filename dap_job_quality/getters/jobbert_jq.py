import logging
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from dap_job_quality import BUCKET_NAME, PROJECT_DIR
from dap_job_quality.getters.data_getters import download_and_extract_from_s3

MODEL_DIR_LOCAL = PROJECT_DIR / "outputs/models/"


def get_jobbert_jq(model_dir=MODEL_DIR_LOCAL, bucket_name=BUCKET_NAME):
    logging.info("Downloading the model...")
    download_and_extract_from_s3(
        "job_quality/sentence_classifier/outputs/jobbert-base-cased-jq-2024-07-15.tar.gz",
        model_dir,
        bucket_name,
    )

    logging.info("Loading the model and tokenizer...")
    model = AutoModelForSequenceClassification.from_pretrained(
        model_dir / "jobbert-base-cased-jq-2024-07-15", num_labels=2
    )
    tokenizer = AutoTokenizer.from_pretrained(
        model_dir / "jobbert-base-cased-jq-2024-07-15"
    )

    return model, tokenizer
