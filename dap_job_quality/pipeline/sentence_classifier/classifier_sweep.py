from datetime import datetime
from dotenv import load_dotenv
import matplotlib

matplotlib.use(
    "Agg"
)  # Use a non-interactive backend for matplotlib so that it (hopefully) works on EC2
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from pathlib import Path
import pickle
from sklearn.decomposition import PCA
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    accuracy_score,
    f1_score,
    recall_score,
)
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from typing import List, Union, Optional
import wandb

from dap_job_quality import BUCKET_NAME, logging, config, PROJECT_DIR
from dap_job_quality.getters.data_getters import load_s3_data, save_to_s3
from dap_job_quality.utils import jobbert

load_dotenv()

CONF_MAT_OUTPATH = PROJECT_DIR / "outputs/figures/log_reg_confusion_matrix.png"
CONF_MAT_OUTPATH.mkdir(parents=True, exist_ok=True)

SEED = 42
N_RUNS = 30

sweep_config = {
    "method": "random",
    "metric": {"name": "recall", "goal": "maximize"},
    "parameters": {
        "embedding_model": {
            "values": [
                "sentence-transformers/all-MiniLM-L6-v2",
                "jjzha/jobbert-base-cased",
            ]
        },
        "penalty": {"values": [None, "l2"]},
        "solver": {"values": ["liblinear", "lbfgs"]},
        "C": {"values": [0.01, 0.1, 1, 10, 100]},
        "max_iter": {"values": [100, 200, 300, 500]},
    },
}

PCA_VAR = 0.95


def record_errors(
    X_val_df: pd.DataFrame,
    y_val: np.ndarray,
    y_pred: np.ndarray,
    type: str = "negatives",
    log_wandb: bool = True,
    outpath: Union[str, Path] = PROJECT_DIR
    / "outputs/data/log_reg_false_negatives.csv",
) -> None:
    """
    Gets the indices of false positives/false negatives and uses these to extract the errors from the original df.

    Parameters:
    - X_val_df (DataFrame): The DataFrame containing the validation data.
    - y_val (ndarray): Array-like structure containing the actual labels.
    - y_pred (ndarray): Array-like structure containing the predicted labels.
    - type (str, optional): Specifies the type of errors to log; 'negatives' logs false negatives,
      any other string logs false positives. Defaults to 'negatives'.
    - wandb (bool, optional): A flag to determine whether to log the errors to Weights & Biases (wandb). Defaults to True.
    - run (Optional[wandb.Run], optional): The Weights & Biases run object to log the errors. Defaults to None.
    - outpath (Union[str, Path], optional): The file path where the error data will be saved as a CSV file.
      Defaults to 'PROJECT_DIR/outputs/data/log_reg_false_negatives.csv'.

    Returns:
    - None: The function does not return any value but writes to a CSV file and logs to wandb if enabled.

    Side Effects:
    - Writes a CSV file to the specified path.
    - Logs data to Weights & Biases if wandb is True and `run` is provided.
    """

    if type == "negatives":
        # false negatives
        errors = np.where((y_val == 1) & (y_pred == 0))[0]
    else:
        # false positives
        errors = np.where((y_val == 0) & (y_pred == 1))[0]

    errors_df = X_val_df.iloc[errors]

    # Ensure the output directory exists
    outpath = Path(outpath)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    errors_df.to_csv(outpath, index=False)

    if log_wandb:
        wb_errors = wandb.Table(data=errors_df)
        wandb.log({f"false_{type}": wb_errors})


def train(config, X_train, X_val_df, y_train, y_val):
    scaler = StandardScaler()

    # Embed sentences
    X_train = jobbert.embed_sentences(X_train, model_name=config["embedding_model"])
    X_val = jobbert.embed_sentences(
        X_val_df["sentence"].tolist(), model_name=config["embedding_model"]
    )

    # Convert embeddings from list of arrays into a single numpy array
    X_train = np.vstack(X_train)
    X_train = scaler.fit_transform(X_train)
    X_val = np.vstack(X_val)
    X_val = scaler.transform(X_val)

    logging.info("Reducing dimensionality with PCA...")
    pca = PCA(n_components=PCA_VAR, random_state=SEED)
    X_train_pca = pca.fit_transform(X_train)
    pickle.dump(
        pca, open(PROJECT_DIR / "outputs/models/sentence_classifier/pca.pkl", "wb")
    )
    save_to_s3(BUCKET_NAME, pca, "job_quality/sentence_classifier/outputs/pca.pkl")
    X_val_pca = pca.transform(X_val)
    logging.info(X_train_pca.shape)

    # Build the model
    model = LogisticRegression(
        C=config["C"],
        max_iter=config["max_iter"],
        penalty=config["penalty"],
        random_state=SEED,
    )
    logging.info("Fitting a logistic regression model...")
    model.fit(X_train_pca, y_train)

    # Predict and evaluate
    preds = model.predict(X_val_pca)
    accuracy = accuracy_score(y_val, preds)
    recall = recall_score(y_val, preds)
    f1 = f1_score(y_val, preds)

    cm = confusion_matrix(y_val, preds)
    cm_df = pd.DataFrame(cm)
    logging.info(cm_df)

    fig, ax = plt.subplots(figsize=(6, 6))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=model.classes_)
    disp.plot(ax=ax)
    plt.savefig(CONF_MAT_OUTPATH, dpi=300)
    wandb.log({"confusion matrix": wandb.Image(str(CONF_MAT_OUTPATH))})

    # Log confusion matrix
    wb_confusion_matrix = wandb.Table(data=cm_df, columns=["0", "1"])
    wandb.log({"confusion_matrix": wb_confusion_matrix})

    logging.info("Recording errors...")
    record_errors(
        X_val_df,
        y_val,
        preds,
        type="negatives",
        log_wandb=True,
        outpath=PROJECT_DIR / "outputs/data/log_reg_false_negatives.csv",
    )
    record_errors(
        X_val_df,
        y_val,
        preds,
        type="positives",
        log_wandb=True,
        outpath=PROJECT_DIR / "outputs/data/log_reg_false_positives.csv",
    )

    return accuracy, recall, f1, cm


def main():
    wandb.init(
        project="dap-job-quality",
        entity="nesta-uk",
        job_type="sentence_classifier",
        save_code=True,
        tags=["logistic regression"],
    )

    logging.info("Loading data...")
    X_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/train_df.parquet"
    ).drop(["label"], axis=1)
    X_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/val_df.parquet"
    ).drop(["label"], axis=1)
    y_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/train_df.parquet"
    )["label"]
    # y_train_writeable = np.copy(y_train)
    y_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/val_df.parquet"
    )["label"]

    # X_val_df = X_val.copy()

    X_train = X_train["sentence"].tolist()
    X_val_df = (
        X_val.copy()
    )  # For the sake of relating predictions back to the original df and recording errors

    accuracy, recall, f1, cm = train(wandb.config, X_train, X_val_df, y_train, y_val)
    wandb.log({"accuracy": accuracy})

    wandb.run.summary["accuracy"] = accuracy
    wandb.run.summary["f1_score"] = f1
    wandb.run.summary["recall"] = recall

    # Log confusion matrix

    # wb_confusion_matrix = wandb.Table(data=cm, columns=["0", "1"])
    # wandb.log({"confusion_matrix": wb_confusion_matrix})


if __name__ == "__main__":
    sweep_id = wandb.sweep(
        sweep=sweep_config, entity="nesta-uk", project="dap-job-quality"
    )

    wandb.agent(sweep_id, function=main, count=30)
