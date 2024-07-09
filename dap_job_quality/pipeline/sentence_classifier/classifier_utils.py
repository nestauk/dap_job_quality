import json
import matplotlib

matplotlib.use(
    "Agg"
)  # Use a non-interactive backend for matplotlib so that it (hopefully) works on EC2
import matplotlib.pyplot as plt
import numpy as np
import os.path
import pandas as pd
from pathlib import Path
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    accuracy_score,
    f1_score,
    recall_score,
    precision_recall_fscore_support,
)
import tarfile
import transformers
from transformers import TrainingArguments, Trainer
import wandb


from dap_job_quality import BUCKET_NAME, logging, PROJECT_DIR
from dap_job_quality.getters.data_getters import load_s3_data, upload_file_to_s3


def log_confusion_matrix_img(cm, outpath, filename):
    outpath.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(6, 6))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm)
    disp.plot(ax=ax)
    plt.savefig(f"{outpath}/{filename}", dpi=300)
    wandb.log({"confusion matrix": wandb.Image(f"{outpath}/{filename}")})


def log_confusion_matrix_table(cm):
    cm_df = pd.DataFrame(cm)
    logging.info(cm_df)

    wb_confusion_matrix = wandb.Table(data=cm_df, columns=["0", "1"])
    wandb.log({"confusion_matrix": wb_confusion_matrix})


def load_datasets_for_hf():
    train_df = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/train_df.parquet"
    )
    val_df = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/val_df.parquet"
    )

    train_df.rename(columns={"label": "labels"}, inplace=True)
    val_df.rename(columns={"label": "labels"}, inplace=True)

    train_df["sentence"] = train_df["sentence"].astype(str)
    val_df["sentence"] = val_df["sentence"].astype(str)

    logging.info(f"Train label distribution: {train_df['labels'].value_counts()}")
    logging.info(f"Val label distribution: {val_df['labels'].value_counts()}")

    return train_df, val_df


def tokenize_function(examples, tokenizer, max_length=128):
    return tokenizer(
        examples["sentence"],
        padding="max_length",
        truncation=True,
        max_length=max_length,
    )


def log_summary_metrics(y_val, y_pred, run):
    run.summary["accuracy"] = accuracy_score(y_val, y_pred)
    run.summary["f1_score"] = f1_score(y_val, y_pred)
    run.summary["recall"] = recall_score(
        y_val, y_pred
    )  # we would like better recall please


def get_best_hyperparams(
    sweep_id,
    eval_metric="eval/recall",
    params=["learning_rate", "batch_size", "weight_decay"],
    outpath=PROJECT_DIR / "outputs/models/",
    outfile="best_varied_hyperparameters.json",
    entity="nesta-uk",
    project_name="dap-job-quality",
):
    # Initialize the W&B API
    api = wandb.Api()

    # Get the sweep
    sweep = api.sweep(f"{entity}/{project_name}/{sweep_id}")

    # Retrieve all runs in the sweep
    runs = sweep.runs

    # Find the run with the best performance
    best_run = None
    best_metric = float("-inf")

    for run in runs:
        metric = run.summary.get(eval_metric)
        if metric is not None and metric > best_metric:
            best_metric = metric
            best_run = run

    if best_run:
        best_hyperparameters = best_run.config
        varied_hyperparameters = {
            key: value for key, value in best_hyperparameters.items() if key in params
        }

        print("Best Hyperparameters:", varied_hyperparameters)

        # Save the varied hyperparameters to a JSON file
        with open(f"{outpath}/{outfile}", "w") as f:
            json.dump(varied_hyperparameters, f, indent=4)

    else:
        print("No best run found.")


def load_training_args(
    **training_args: dict,
) -> transformers.training_args.TrainingArguments:
    """Load Training Arguments to be used to train the model

    Args:
        training_args: Dictionary of training arguments

    Returns:
        TrainingArguments object
    """

    return TrainingArguments(**training_args)


def make_tarfile(output_filename, source_dir):
    """Creates a tarfile from a folder

    Args:
        output_filename (str): Name of the file to be saved
        source_dir (str): Path to the folder to be zipped

    Returns:
        None: Saves the tarfile locally
    """
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def saving_huggingface_model(
    trainer: Trainer, output_filename: str, save_path: str, s3_path: str
):
    """Saves a huggingface model to S3

    Args:
        trainer (transformers.Trainer): Trained huggingface trainer
        output_filename (str): Name of the file to be saved
        save_path (str): Path to save the model to

    Returns:
        None: Saves the model locally and uploads to S3
    """
    if isinstance(save_path, str):
        save_path = Path(save_path)
    # Saving the model locally as a folder
    save_path.mkdir(parents=True, exist_ok=True)
    model_path = Path.joinpath(save_path, output_filename)
    trainer.save_model(model_path)
    # Converting folder to zipped file
    make_tarfile(str(model_path) + ".tar.gz", str(model_path))
    # Uploading to S3
    upload_file_to_s3(
        file_name=str(model_path) + ".tar.gz",
        bucket=BUCKET_NAME,
        object_name=f"{s3_path}{output_filename}.tar.gz",
    )


def saving_huggingface_tokenizer(
    tokenizer, output_filename: str, save_path: str, s3_path: str
):
    """Saves a huggingface tokenizer to S3

    Args:
        tokenizer (transformers.PreTrainedTokenizer): Tokenizer to save
        output_filename (str): Name of the file to be saved
        save_path (str): Path to save the tokenizer to
        s3_path (str): S3 path to upload the tokenizer to

    Returns:
        None: Saves the tokenizer locally and uploads to S3
    """
    if isinstance(save_path, str):
        save_path = Path(save_path)
    # Saving the tokenizer locally as a folder
    save_path.mkdir(parents=True, exist_ok=True)
    tokenizer_path = save_path / output_filename
    tokenizer.save_pretrained(tokenizer_path)

    # Converting folder to zipped file
    tarball_path = str(tokenizer_path) + ".tar.gz"
    make_tarfile(tarball_path, str(tokenizer_path))

    # Uploading to S3
    upload_file_to_s3(
        path_from=tarball_path,
        bucket=BUCKET_NAME,
        path_to=f"{s3_path}{output_filename}.tar.gz",
    )


def compute_metrics(p):
    """ """
    preds = np.argmax(p.predictions, axis=1)
    precision, recall, f1, _ = precision_recall_fscore_support(
        p.label_ids, preds, average="binary"
    )
    acc = accuracy_score(p.label_ids, preds)
    return {"accuracy": acc, "f1": f1, "precision": precision, "recall": recall}
