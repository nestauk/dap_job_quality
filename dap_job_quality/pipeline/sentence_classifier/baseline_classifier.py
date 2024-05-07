from dotenv import load_dotenv
import numpy as np
import os
import pandas as pd
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
    f1_score,
)
import wandb

from dap_job_quality import BUCKET_NAME, logging, config
from dap_job_quality.getters.data_getters import load_s3_data

load_dotenv()

SEED = config["seed"]
WANDB_ENTITY = os.getenv("WANDB_ENTITY")

np.random.seed(SEED)

if __name__ == "__main__":
    y_train = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/y_train.pkl"
    )
    y_val = load_s3_data(
        BUCKET_NAME, "job_quality/sentence_classifier/inputs/labelled/y_val.pkl"
    )

    run = wandb.init(
        project="dap-job-quality",
        entity=WANDB_ENTITY,
        job_type="Sentence classifier",
        save_code=True,
        tags=[f"baseline_model"],
    )

    label_probabilities = y_train.value_counts(normalize=True)

    y_pred = np.random.choice(
        [0, 1], size=len(y_val), p=[label_probabilities[0], label_probabilities[1]]
    )

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
