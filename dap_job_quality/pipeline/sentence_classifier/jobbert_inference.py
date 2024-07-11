"""
This script shows how to use the fine-tuned jobbert model for inference.

The output gets saved to `PROJECT_DIR / f"outputs/data/jobbert_test_n_samples_{args.n_samples}.csv"` so
you can check the output there if you wish.

Usage:
```
python dap_job_quality/pipeline/sentence_classifier/jobbert_inference.py --n_samples=10
```
"""
import argparse
import logging
import nltk
from nltk.tokenize import sent_tokenize
import pandas as pd
from time import time
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from typing import List, Union, Tuple

from dap_job_quality import PROJECT_DIR, BUCKET_NAME
from dap_job_quality.getters.afs_data import get_stratified_sample
from dap_job_quality.getters.data_getters import download_and_extract_from_s3

MODEL_DIR_LOCAL = PROJECT_DIR / "outputs/data/models/"

nltk.download("punkt")
nltk.download("stopwords")


def extract_job_quality_sentences(
    job_adverts: Union[pd.DataFrame, List[str], str],
    id_col: str = "id",
    text_col: str = "clean_description",
) -> List[str]:
    """Convert job advert dataframe to list of texts to use for inference.

    Args:
        job_adverts (Union[pd.DataFrame, List[str], str]): Dataframe of job ads
        id_col (str, optional): Unique identifier of each ad. Defaults to "id".
        text_col (str, optional): Name of the column containing the job description. Defaults to "clean_description".

    Returns:
        List[str]: List of sentences from job adverts.
    """
    if isinstance(job_adverts, str):
        jobs_df = pd.DataFrame([{id_col: 0, text_col: job_adverts}])
    elif isinstance(job_adverts, list):
        jobs_df = (
            pd.DataFrame(job_adverts, columns=[text_col])
            .reset_index()
            .rename(columns={"index": id_col})
        )
    elif isinstance(job_adverts, pd.DataFrame):
        jobs_df = job_adverts[[id_col, text_col]].copy()
    else:
        raise ValueError("Input must be a DataFrame, a list of strings, or a string")

    jobs_df["sentences"] = jobs_df[text_col].apply(lambda x: sent_tokenize(x))
    jobs_df = jobs_df.explode("sentences")

    return jobs_df["sentences"].tolist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--n_samples",
        default=1,
        type=int,
        help="Number of samples to test the pipeline on",
    )

    args = parser.parse_args()
    logging.info(args)

    logging.info("Loading data...")
    job_adverts = get_stratified_sample()

    if args.n_samples <= len(job_adverts):
        job_adverts = job_adverts.sample(args.n_samples)
    else:
        logging.info(
            f"Number of samples requested exceeds the total number of samples. Returning all {len(job_adverts)} samples."
        )

    input_texts = extract_job_quality_sentences(job_adverts)

    logging.info("Downloading the model...")
    download_and_extract_from_s3(
        "job_quality/sentence_classifier/outputs/jobbert-base-cased-jq.tar.gz",
        MODEL_DIR_LOCAL,
        BUCKET_NAME,
    )

    logging.info("Loading the model and tokenizer...")
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_DIR_LOCAL / "jobbert-base-cased-jq", num_labels=2
    )
    tokenizer = AutoTokenizer.from_pretrained(MODEL_DIR_LOCAL / "jobbert-base-cased-jq")

    # Tokenize the new data
    logging.info("Classifying texts...")
    start_time = time()

    classifier = pipeline(
        "text-classification",
        model=model,
        tokenizer=tokenizer,
        return_all_scores=False,
        device=0 if torch.cuda.is_available() else -1,  # Use GPU if available
    )

    predictions = classifier(input_texts)

    end_time = time()

    logging.info(
        f"Time taken for inference on {len(job_adverts)} texts: {end_time - start_time} seconds"
    )

    logging.info("Converting predictions to labels...")
    predicted_labels = [pred["label"] for pred in predictions]

    logging.info("Saving predictions to CSV...")
    df = pd.DataFrame({"input_text": input_texts, "predicted_label": predicted_labels})
    df.to_csv(
        PROJECT_DIR / f"outputs/data/jobbert_test_n_samples_{args.n_samples}.csv",
        index=False,
    )
