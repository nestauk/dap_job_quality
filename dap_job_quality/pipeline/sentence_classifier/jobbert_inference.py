"""
This script shows how to use the fine-tuned jobbert model for inference.
"""
import argparse
import logging
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from dap_job_quality import PROJECT_DIR, BUCKET_NAME
from dap_job_quality.getters.data_getters import download_and_extract_from_s3

MODEL_DIR_LOCAL = PROJECT_DIR / "outputs/data/models/"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--input_text",
        default="You will receive £1 million per hour.",
        type=str,
        help="Text to classify",
    )

    args = parser.parse_args()
    logging.info(args)

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
    logging.info("Tokenizing the input text...")
    inputs = tokenizer(
        args.input_text,
        truncation=True,
        padding=True,
        return_tensors="pt",
        max_length=128,
    )

    logging.info("Making predictions...")
    model.eval()
    with torch.no_grad():
        outputs = model(**inputs)
        predictions = torch.argmax(outputs.logits, dim=-1)

    logging.info("Converting predictions to labels...")
    predicted_labels = predictions.numpy()

    logging.info(
        f"Input text: {args.input_text} \n \n Predicted label: {predicted_labels}"
    )
