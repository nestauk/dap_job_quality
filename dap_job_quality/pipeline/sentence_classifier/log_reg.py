import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
    f1_score,
)
from sklearn.linear_model import LogisticRegression
import wandb

from dap_job_quality import BUCKET_NAME, logging
from dap_job_quality.getters.data_getters import load_s3_data

LOG_REG_PARAMS = {
    "penalty": "l2",
    "solver": "liblinear",
    "random_state": 42,
    "max_iter": 100,
}

PCA_VAR = 0.95

if __name__ == "__main__":
    X_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/X_train_dummy.pkl"
    )
    X_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/X_val_dummy.pkl"
    )
    y_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/y_train_dummy.pkl"
    )
    y_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/y_val_dummy.pkl"
    )

    # Convert embeddings from list of arrays into a single numpy array
    X_train = np.vstack(X_train)
    X_val = np.vstack(X_val)

    run = wandb.init(
        project="dap-job-quality",
        job_type="Sentence classifier",
        save_code=True,
        tags=[f"logistic_regression"],
    )

    # Dimensionality Reduction with PCA
    pca = PCA(n_components=PCA_VAR)  # retains 95% of the variance
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
    cm = pd.DataFrame(cm)
    logging.info(cm)

    # Log confusion matrix
    wb_confusion_matrix = wandb.Table(data=cm, columns=["0", "1"])
    run.log({"confusion_matrix": wb_confusion_matrix})

    # End the weights and biases run
    wandb.finish()
