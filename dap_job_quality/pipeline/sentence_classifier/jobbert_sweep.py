import argparse
import matplotlib

matplotlib.use(
    "Agg"
)  # Use a non-interactive backend for matplotlib so that it (hopefully) works on EC2
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random
from scipy.special import softmax
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
    AutoTokenizer,
    AutoModelForSequenceClassification,
    DataCollatorWithPadding,
    TrainingArguments,
    Trainer,
)
from datasets import load_dataset, load_metric, Dataset

from dap_job_quality import BUCKET_NAME, logging, PROJECT_DIR, get_yaml_config
from dap_job_quality.getters.data_getters import load_s3_data
from dap_job_quality.utils import jobbert
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

jobbert_config = get_yaml_config(
    PROJECT_DIR / "dap_job_quality/pipeline/sentence_classifier/jobbert_config.yaml"
)

CONF_MAT_OUTPATH = PROJECT_DIR / "outputs/figures/"
MODEL_NAME = "jjzha/jobbert-base-cased"

model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2)
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)


def model_train():
    run = wandb.init()
    run.log_code()

    train_df, val_df = load_datasets_for_hf()

    # Convert DataFrames to Datasets
    train_dataset = Dataset.from_pandas(train_df)
    val_dataset = Dataset.from_pandas(val_df)

    tokenized_train_dataset = train_dataset.map(
        tokenize_function,
        batched=True,
        fn_kwargs={"tokenizer": tokenizer, "max_length": 128},
    )
    tokenized_val_dataset = val_dataset.map(
        tokenize_function,
        batched=True,
        fn_kwargs={"tokenizer": tokenizer, "max_length": 128},
    )

    # data collator
    data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

    training_args = TrainingArguments(
        output_dir="jobbert-base-cased-jq",
        learning_rate=wandb.config.learning_rate,
        per_device_train_batch_size=wandb.config.batch_size,
        per_device_eval_batch_size=wandb.config.batch_size,
        num_train_epochs=1,
        weight_decay=wandb.config.weight_decay,
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_train_dataset,
        eval_dataset=tokenized_val_dataset,
        tokenizer=tokenizer,
        data_collator=data_collator,
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
        jobbert_config["sweep_config"], project="dap-job-quality", entity="nesta-uk"
    )
    wandb.agent(sweep_id, function=model_train, count=args.n_runs)

    get_best_hyperparams(
        sweep_id,
        eval_metric="recall",
        params=["learning_rate", "batch_size", "weight_decay"],
        outpath=PROJECT_DIR / "outputs/models/",
        outfile=f"jobbert_best_varied_hyperparameters_sweep_{sweep_id}.json",
        entity="nesta-uk",
        project_name="dap-job-quality",
    )
