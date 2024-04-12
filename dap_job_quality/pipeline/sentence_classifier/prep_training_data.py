from datetime import date
import json
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

# from sklearn.linear_model import LogisticRegression
# from sklearn.metrics import classification_report, confusion_matrix, ConfusionMatrixDisplay
from transformers import AutoTokenizer, AutoModel
import torch

from dap_job_quality import PROJECT_DIR, config, BUCKET_NAME
from dap_job_quality.getters.labelled_data import get_dummy_job_sentences
from dap_job_quality.getters.data_getters import save_to_s3

SENT_MODEL = config["sentence_model"]
SEED = 42


def embed_sentences(sentences, model_name=SENT_MODEL):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name)
    embeddings = []

    for sentence in sentences:
        inputs = tokenizer(
            sentence, return_tensors="pt", padding=True, truncation=True, max_length=512
        )
        with torch.no_grad():
            outputs = model(**inputs)
        # Use the mean of the last hidden state as the sentence embedding
        embeddings.append(outputs.last_hidden_state.mean(dim=1).squeeze().numpy())

    return embeddings


if __name__ == "__main__":
    data = get_dummy_job_sentences()

    sent_df = pd.DataFrame(data)

    sent_df_filtered = sent_df[sent_df["label"] != -1]

    sent_df_filtered["embeddings"] = embed_sentences(sent_df_filtered["sentence"])

    # Splitting the dataset into training, validation, and test sets
    X_train, X_temp, y_train, y_temp = train_test_split(
        sent_df_filtered["embeddings"].tolist(),
        sent_df_filtered["label"],
        test_size=0.4,
        random_state=SEED,
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.5, random_state=SEED
    )

    for data, name in zip(
        [X_train, X_val, X_test, y_train, y_val, y_test],
        ["X_train", "X_val", "X_test", "y_train", "y_val", "y_test"],
    ):
        save_to_s3(
            BUCKET_NAME,
            data,
            f"job_quality/sentence_classifier/inputs/labelled/{name}_dummy.pkl",
        )
