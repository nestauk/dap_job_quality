import argparse
import matplotlib

matplotlib.use(
    "Agg"
)  # Use a non-interactive backend for matplotlib so that it (hopefully) works on EC2
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    accuracy_score,
    f1_score,
    recall_score,
    precision_recall_fscore_support,
)
import wandb

# huggingface code
from transformers import (
    DistilBertTokenizerFast,
    DistilBertForSequenceClassification,
    TrainingArguments,
    Trainer,
)
from datasets import load_dataset, load_metric, Dataset

from dap_job_quality import BUCKET_NAME, logging, PROJECT_DIR, get_yaml_config
from dap_job_quality.getters.data_getters import load_s3_data
from dap_job_quality.pipeline.sentence_classifier.classifier_utils import (
    load_datasets_for_hf,
    log_confusion_matrix_img,
    tokenize_function,
    log_confusion_matrix_table,
    log_summary_metrics,
    get_best_hyperparams,
)


SEED = 42
random.seed(SEED)
np.random.seed(SEED)

distilbert_config = get_yaml_config(
    PROJECT_DIR / "dap_job_quality/pipeline/sentence_classifier/distilbert_config.yaml"
)

CONF_MAT_OUTPATH = PROJECT_DIR / "outputs/figures/"

# Load the tokenizer
tokenizer = DistilBertTokenizerFast.from_pretrained(distilbert_config["model"])
max_length = distilbert_config["max_length"]


def model_train(distilbert_model=distilbert_config["model"]):
    run = wandb.init()

    train_df, val_df = load_datasets_for_hf()

    # Convert DataFrames to Datasets
    train_dataset = Dataset.from_pandas(train_df)
    val_dataset = Dataset.from_pandas(val_df)

    tokenized_train_dataset = train_dataset.map(
        tokenize_function,
        batched=True,
        fn_kwargs={"tokenizer": tokenizer, "max_length": max_length},
    )
    tokenized_val_dataset = val_dataset.map(
        tokenize_function,
        batched=True,
        fn_kwargs={"tokenizer": tokenizer, "max_length": max_length},
    )

    tokenized_train_dataset.set_format(
        "torch", columns=["input_ids", "attention_mask", "labels"]
    )
    tokenized_val_dataset.set_format(
        "torch", columns=["input_ids", "attention_mask", "labels"]
    )

    training_args = TrainingArguments(
        output_dir="./results",
        evaluation_strategy="epoch",
        learning_rate=wandb.config.learning_rate,
        per_device_train_batch_size=wandb.config.batch_size,
        per_device_eval_batch_size=wandb.config.batch_size,
        num_train_epochs=wandb.config.epochs,
        weight_decay=wandb.config.weight_decay,
        logging_dir="./logs",
    )
    # Load the model
    model = DistilBertForSequenceClassification.from_pretrained(
        distilbert_model, num_labels=2
    )

    # Create a Trainer instance
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_train_dataset,
        eval_dataset=tokenized_val_dataset,
    )

    # Train the model
    trainer.train()

    # Evaluate the model
    eval_results = trainer.evaluate()

    print(f"Eval results: {eval_results}")

    # Make predictions on the validation set
    predictions = trainer.predict(tokenized_val_dataset)

    # Get the predicted labels
    predicted_labels = np.argmax(predictions.predictions, axis=1)

    # Add predicted labels and true labels to the validation dataframe
    val_df["predicted_labels"] = predicted_labels

    log_summary_metrics(
        y_val=val_df["labels"], y_pred=val_df["predicted_labels"], run=run
    )

    # Identify errors
    val_df["error"] = val_df["predicted_labels"] != val_df["labels"]

    # Create a dataframe with errors
    val_errors = val_df[val_df["error"] == True]

    # Upload val_errors to W&B as an artifact
    val_errors_artifact = wandb.Artifact("validation_errors", type="dataset")
    val_errors_artifact.add(wandb.Table(dataframe=val_errors), "val_errors")
    wandb.log_artifact(val_errors_artifact)

    # confusion matrix
    cm = confusion_matrix(val_df["labels"], val_df["predicted_labels"])
    log_confusion_matrix_img(cm, CONF_MAT_OUTPATH, "distilbert_confusion_matrix.png")
    log_confusion_matrix_table(cm)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--n_runs",
        default=100,
        type=int,
        help="How many runs to do as part of the sweep?",
    )

    args = parser.parse_args()
    logging.info(args)

    sweep_id = wandb.sweep(
        distilbert_config["sweep_config"], project="dap-job-quality", entity="nesta-uk"
    )
    wandb.agent(sweep_id, function=model_train, count=args.n_runs)

    get_best_hyperparams(
        sweep_id,
        eval_metric="recall",
        params=["learning_rate", "batch_size", "weight_decay"],
        outpath=PROJECT_DIR / "outputs/models/",
        outfile=f"distilbert_best_varied_hyperparameters_sweep_{sweep_id}.json",
        entity="nesta-uk",
        project_name="dap-job-quality",
    )
