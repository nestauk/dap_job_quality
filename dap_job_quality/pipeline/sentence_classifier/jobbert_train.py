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
    EarlyStoppingCallback,
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
    load_training_args,
    saving_huggingface_model,
    compute_metrics,
    saving_huggingface_tokenizer,
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

if __name__ == "__main__":
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
        output_dir=PROJECT_DIR / "outputs/models/",
        learning_rate=jobbert_config["train_config"]["learning_rate"],
        per_device_train_batch_size=jobbert_config["train_config"][
            "per_device_train_batch_size"
        ],
        per_device_eval_batch_size=jobbert_config["train_config"][
            "per_device_eval_batch_size"
        ],
        gradient_accumulation_steps=jobbert_config["train_config"][
            "gradient_accumulation_steps"
        ],
        num_train_epochs=jobbert_config["train_config"]["num_train_epochs"],
        evaluation_strategy=jobbert_config["train_config"]["evaluation_strategy"],
        save_strategy=jobbert_config["train_config"]["save_strategy"],
        metric_for_best_model=jobbert_config["train_config"]["metric_for_best_model"],
        load_best_model_at_end=jobbert_config["train_config"]["load_best_model_at_end"],
        seed=jobbert_config["train_config"]["seed"],
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
    run.log({f"false_{type}": wb_errors})

    SAVE_TRAINING_RESULTS_PATH = (
        PROJECT_DIR / "outputs/models/sentence_classifier/jobbert-base-cased-jq"
    )
    trainer.save_model(SAVE_TRAINING_RESULTS_PATH)
    tokenizer.save_pretrained(SAVE_TRAINING_RESULTS_PATH)

    wandb.finish()
