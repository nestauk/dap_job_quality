from datetime import date
import json
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from transformers import AutoTokenizer, AutoModel
import torch
from typing import List

from dap_job_quality import PROJECT_DIR, config, BUCKET_NAME
from dap_job_quality.getters.labelled_data import get_dummy_job_sentences
from dap_job_quality.getters.data_getters import save_to_s3

SENT_MODEL = config["sentence_model"]
SEED = 42


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


if __name__ == "__main__":
    data = get_dummy_job_sentences()

    sent_df = pd.DataFrame(data)

    sent_df_filtered = sent_df[sent_df["label"] != -1]

    embeddings = embed_sentences(sent_df_filtered["sentence"].tolist())

    sent_df_filtered["embeddings"] = embeddings.tolist()

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
