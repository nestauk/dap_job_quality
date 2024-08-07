import argparse
from datetime import datetime

import numpy as np
import os
import random
from scipy.special import softmax
import shutil
from sklearn.metrics import confusion_matrix
import wandb


# huggingface code
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    DataCollatorWithPadding,
    TrainingArguments,
    Trainer,
    EarlyStoppingCallback,
)
from datasets import Dataset

from dap_job_quality import logging, PROJECT_DIR, get_yaml_config
from dap_job_quality.pipeline.sentence_classifier.classifier_utils import (
    load_datasets_for_hf,
    log_confusion_matrix_img,
    tokenize_function,
    log_confusion_matrix_table,
    log_summary_metrics,
    saving_huggingface_model,
    compute_metrics,
)

jobbert_config = get_yaml_config(
    PROJECT_DIR / "dap_job_quality/config/jobbert_config.yaml"
)

TODAY = datetime.today().strftime("%Y-%m-%d")
SEED = jobbert_config["seed"]
random.seed(SEED)
np.random.seed(SEED)

CONF_MAT_OUTPATH = PROJECT_DIR / "outputs/figures/"
INPUT_MODEL_NAME = "jjzha/jobbert-base-cased"

OUTPUT_MODEL_NAME = f"jobbert-base-cased-jq-{TODAY}"
LOCAL_SAVE_PATH = (
    PROJECT_DIR / f"outputs/models/sentence_classifier/{OUTPUT_MODEL_NAME}"
)
S3_SAVE_PATH = "job_quality/sentence_classifier/outputs/"
CHECKPOINT_DIR = PROJECT_DIR / "outputs/models/sentence_classifier/checkpoints/"

MAX_LENGTH = jobbert_config["max_length"]

if os.path.exists(CHECKPOINT_DIR):
    shutil.rmtree(CHECKPOINT_DIR)
os.makedirs(CHECKPOINT_DIR)

model = AutoModelForSequenceClassification.from_pretrained(
    INPUT_MODEL_NAME, num_labels=2
)
tokenizer = AutoTokenizer.from_pretrained(INPUT_MODEL_NAME)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--production",
        default=False,
        type=bool,
        help="Run in production mode and save the model?",
    )

    args = parser.parse_args()
    logging.info(args)

    logging.info("Logging run on wandb")
    run = wandb.init(
        reinit=True,
        entity="nesta-uk",
        project="dap-job-quality",
        job_type="Sentence classifier",
        save_code=True,
        tags=[
            "jobbert",
        ],
    )

    train_df, val_df = load_datasets_for_hf()

    # Convert DataFrames to Datasets
    train_dataset = Dataset.from_pandas(train_df)
    val_dataset = Dataset.from_pandas(val_df)

    tokenized_train_dataset = train_dataset.map(
        tokenize_function,
        batched=True,
        fn_kwargs={"tokenizer": tokenizer, "max_length": MAX_LENGTH},
    )
    tokenized_val_dataset = val_dataset.map(
        tokenize_function,
        batched=True,
        fn_kwargs={"tokenizer": tokenizer, "max_length": MAX_LENGTH},
    )

    # data collator
    data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

    training_args = TrainingArguments(
        output_dir=CHECKPOINT_DIR,
        learning_rate=jobbert_config["train_config"]["learning_rate"],
        per_device_train_batch_size=jobbert_config["train_config"][
            "per_device_train_batch_size"
        ],
        per_device_eval_batch_size=jobbert_config["train_config"][
            "per_device_eval_batch_size"
        ],
        weight_decay=jobbert_config["train_config"]["weight_decay"],
        num_train_epochs=jobbert_config["train_config"]["num_train_epochs"],
        evaluation_strategy=jobbert_config["train_config"]["evaluation_strategy"],
        save_strategy=jobbert_config["train_config"]["save_strategy"],
        metric_for_best_model=jobbert_config["train_config"]["metric_for_best_model"],
        load_best_model_at_end=jobbert_config["train_config"]["load_best_model_at_end"],
        seed=jobbert_config["seed"],
        report_to="wandb",
    )

    early_stopping_callback = EarlyStoppingCallback(
        early_stopping_patience=jobbert_config["train_config"][
            "early_stopping_patience"
        ]
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_train_dataset,
        eval_dataset=tokenized_val_dataset,
        tokenizer=tokenizer,
        data_collator=data_collator,
        compute_metrics=compute_metrics,
        callbacks=[early_stopping_callback],
    )

    trainer.train()

    # Evaluate the model
    eval_results = trainer.evaluate()

    print(f"Eval results: {eval_results}")

    # convert logits to probabilities
    probs = softmax(trainer.predict(tokenized_val_dataset).predictions, axis=1)
    # convert probabilities to predicted labels
    y_pred = np.argmax(probs, axis=-1)
    # get true labels
    y_true = tokenized_val_dataset["labels"]

    log_summary_metrics(y_val=y_true, y_pred=y_pred, run=run)

    cm = confusion_matrix(y_true, y_pred)

    log_confusion_matrix_img(cm, CONF_MAT_OUTPATH, "jobbert_confusion_matrix.png")
    log_confusion_matrix_table(cm)

    val_df["predictions"] = y_pred
    errors = val_df[val_df["labels"] != val_df["predictions"]]
    wb_errors = wandb.Table(data=errors)
    run.log({f"errors": wb_errors})

    if args.production == True:
        saving_huggingface_model(
            trainer, OUTPUT_MODEL_NAME, save_path=LOCAL_SAVE_PATH, s3_path=S3_SAVE_PATH
        )

    wandb.finish()
