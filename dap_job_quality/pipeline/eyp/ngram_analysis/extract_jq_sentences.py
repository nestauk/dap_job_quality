import boto3
import numpy as np
import os
import pandas as pd
import torch

from dap_job_quality import PROJECT_DIR, BUCKET_NAME, logging
from dap_job_quality.getters.models import (
    sentence_classifier_pca,
    sentence_classifier_lr,
)
from dap_job_quality.getters.data_getters import load_s3_data, save_to_s3
from dap_job_quality.getters.afs_data import (
    ngram_analysis_embeddings,
    ngram_analysis_identifiers,
)

model = sentence_classifier_lr()
pca = sentence_classifier_pca()

THRESHOLD = 0.3

if __name__ == "__main__":
    ad_embeddings = ngram_analysis_embeddings()
    identifiers = ngram_analysis_identifiers()

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
