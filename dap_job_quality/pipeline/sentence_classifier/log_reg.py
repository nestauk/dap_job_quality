from datetime import datetime
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from pathlib import Path
from sklearn.decomposition import PCA
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    ConfusionMatrixDisplay,
    accuracy_score,
    f1_score,
)
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from transformers import AutoTokenizer, AutoModel
import torch
from typing import List, Union, Optional
import wandb

from dap_job_quality import BUCKET_NAME, logging, config, PROJECT_DIR
from dap_job_quality.getters.data_getters import load_s3_data

load_dotenv()

SENT_MODEL = config["sentence_model"]
CONF_MAT_OUTPATH = PROJECT_DIR / "outputs/figures/log_reg_confusion_matrix.png"
WANDB_ENTITY = os.getenv("WANDB_ENTITY")

LOG_REG_PARAMS = {
    "penalty": "l2",
    "solver": "liblinear",
    "random_state": 42,
    "max_iter": 100,
}

PCA_VAR = 0.95


# Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask


def embed_sentences(
    sentences: List[str], model_name: str = SENT_MODEL
) -> List[torch.Tensor]:
    """
    Generate embeddings for each sentence in a list of sentences using a specified model.

    Follows the method described here: https://www.sbert.net/examples/applications/computing-embeddings/README.html

    Args:
        sentences (List[str]): A list of sentences to be embedded.
        model_name (str): The name of the model to use for generating embeddings. Default
                          is a globally defined variable `SENT_MODEL`.

    Returns:
        List[torch.Tensor]: A list of tensors where each tensor represents the embedding
                            of a corresponding sentence in the input list.
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)

    # Tokenize sentences
    encoded_input = tokenizer(
        sentences, padding=True, truncation=True, max_length=512, return_tensors="pt"
    )

    # Compute token embeddings
    with torch.no_grad():
        model_output = model(**encoded_input)

    # Perform pooling. In this case, mean pooling
    embeddings = mean_pooling(model_output, encoded_input["attention_mask"])

    return embeddings


def record_errors(
    X_val_df: pd.DataFrame,
    y_val: np.ndarray,
    y_pred: np.ndarray,
    type: str = "negatives",
    log_wandb: bool = True,
    run: Optional[wandb.sdk.wandb_run.Run] = None,
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

    errors_df.to_csv(outpath, index=False)
    if log_wandb and run is not None:
        wb_errors = wandb.Table(data=errors_df)
        run.log({f"false_{type}": wb_errors})


if __name__ == "__main__":
    X_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/X_train.pkl"
    )
    X_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/X_val.pkl"
    )
    y_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/y_train.pkl"
    )
    y_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/y_val.pkl"
    )

    X_val_df = X_val.copy()

    X_train = X_train["sentence"].tolist()
    X_val = X_val["sentence"].tolist()

    scaler = StandardScaler()

    # Embed sentences
    X_train = embed_sentences(X_train)
    X_val = embed_sentences(X_val)

    # Convert embeddings from list of arrays into a single numpy array
    X_train = np.vstack(X_train)
    X_train = scaler.fit_transform(X_train)
    X_val = np.vstack(X_val)
    X_val = scaler.transform(X_val)

    run = wandb.init(
        project="dap-job-quality",
        entity=WANDB_ENTITY,
        job_type="Sentence classifier",
        save_code=True,
        tags=[f"logistic_regression"],
    )

    # Dimensionality Reduction with PCA
    pca = PCA(n_components=PCA_VAR, random_state=LOG_REG_PARAMS["random_state"])
    X_train_pca = pca.fit_transform(X_train)
    X_val_pca = pca.transform(X_val)
    logging.info(X_train_pca.shape)

    # Train a logistic regression model
    model = LogisticRegression(
        penalty=LOG_REG_PARAMS["penalty"],
        solver=LOG_REG_PARAMS["solver"],
        random_state=LOG_REG_PARAMS["random_state"],
        max_iter=LOG_REG_PARAMS["max_iter"],
    )
    model.fit(X_train_pca, y_train)

    # Evaluate the model on the validation set
    y_pred = model.predict(X_val_pca)
    logging.info(classification_report(y_val, y_pred))

    wandb.run.summary["accuracy"] = accuracy_score(y_val, y_pred)
    wandb.run.summary["f1_score"] = f1_score(y_val, y_pred)

    cm = confusion_matrix(y_val, y_pred)
    cm_df = pd.DataFrame(cm)
    logging.info(cm_df)

    fig, ax = plt.subplots(figsize=(6, 6))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=model.classes_)
    disp.plot(ax=ax)
    plt.savefig(CONF_MAT_OUTPATH, dpi=300)
    wandb.log({"confusion matrix": wandb.Image(str(CONF_MAT_OUTPATH))})

    # Log confusion matrix
    wb_confusion_matrix = wandb.Table(data=cm_df, columns=["0", "1"])
    run.log({"confusion_matrix": wb_confusion_matrix})

    record_errors(
        X_val_df,
        y_val,
        y_pred,
        type="negatives",
        log_wandb=True,
        run=run,
        outpath=PROJECT_DIR / "outputs/data/log_reg_false_negatives.csv",
    )
    record_errors(
        X_val_df,
        y_val,
        y_pred,
        type="positives",
        log_wandb=True,
        run=run,
        outpath=PROJECT_DIR / "outputs/data/log_reg_false_positives.csv",
    )

    # End the weights and biases run
    wandb.finish()
