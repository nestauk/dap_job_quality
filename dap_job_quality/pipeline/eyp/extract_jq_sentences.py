import boto3
import numpy as np
import pandas as pd
import torch

from dap_job_quality import PROJECT_DIR, BUCKET_NAME, logging
from dap_job_quality.getters.models import (
    sentence_classifier_pca,
    sentence_classifier_lr,
)
from dap_job_quality.getters.data_getters import load_s3_data, save_to_s3

OBJECT_NAME_EMBEDDINGS = "job_quality/early_years/embeddings_2023.npy"
FILE_NAME_EMBEDDINGS = PROJECT_DIR / "inputs/eyp/embeddings_2023.npy"
OBJECT_NAME_IDENTIFIERS = "job_quality/early_years/identifiers_2023.npy"
FILE_NAME_IDENTIFIERS = PROJECT_DIR / "inputs/eyp/identifiers_2023.npy"

model = sentence_classifier_lr()
pca = sentence_classifier_pca()

THRESHOLD = 0.3


def download_from_s3(bucket_name, object_name, file_name):
    s3_client = boto3.client("s3")
    s3_client.download_file(bucket_name, object_name, file_name)


def load_embeddings_numpy(file_name: str) -> torch.Tensor:
    np_array = np.load(file_name)
    tensor = torch.from_numpy(np_array)
    return tensor


def load_identifiers_numpy(file_name: str) -> np.ndarray:
    return np.load(file_name)


if __name__ == "__main__":
    download_from_s3(BUCKET_NAME, OBJECT_NAME_EMBEDDINGS, FILE_NAME_EMBEDDINGS)
    download_from_s3(BUCKET_NAME, OBJECT_NAME_IDENTIFIERS, FILE_NAME_IDENTIFIERS)

    ad_embeddings = load_embeddings_numpy(FILE_NAME_EMBEDDINGS)
    identifiers = load_identifiers_numpy(FILE_NAME_IDENTIFIERS)

    X_new_pca = pca.transform(ad_embeddings)

    predictions = model.predict_proba(X_new_pca)[:, 1]

    predictions_df = pd.DataFrame(
        {"identifier": identifiers, "job_quality": predictions}
    )

    all_job_ads = load_s3_data(
        BUCKET_NAME, "job_quality/early_years/all_ads_2023.parquet"
    )
    all_job_ads = all_job_ads.merge(predictions_df, on="identifier")

    all_job_ads["job_quality"].hist(bins=100)

    logging.info(all_job_ads["job_quality"].describe())

    jq_sentences_df = all_job_ads[all_job_ads["job_quality"] >= THRESHOLD]

    logging.info(
        f"N sentences identified as being about job quality: {len(jq_sentences_df)}"
    )
    logging.info(
        f"As a proportion, this is {round(len(jq_sentences_df) / len(all_job_ads), 3)}"
    )

    save_to_s3(
        BUCKET_NAME,
        jq_sentences_df,
        "job_quality/early_years/jq_sentences/jq_sentences_df_2023.parquet",
    )
