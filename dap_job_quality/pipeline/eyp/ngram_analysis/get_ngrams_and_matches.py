from datetime import datetime
import pandas as pd
from sentence_transformers import SentenceTransformer

from dap_job_quality import logging, BUCKET_NAME
from dap_job_quality.getters.afs_data import get_jq_sentences
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.getters.keywords import get_keywords
from dap_job_quality.pipeline.find_job_quality import (
    extract_ngrams,
    match_to_lookup,
)

sent_model = SentenceTransformer("all-MiniLM-L6-v2")

today = datetime.today().strftime("%Y-%m-%d")

JQ_THRESHOLD = 0.3

if __name__ == "__main__":
    # download sample of sentences that we've classified as relating to job quality
    jq_sentences_df = get_jq_sentences()

    # load in the lookup table
    lookup = get_keywords()

    jq_sentences_df_long = extract_ngrams(jq_sentences_df, "sentences")

    # calculate cosine similarity of unique ngrams to target phrases
    ngram_counts = pd.DataFrame(
        jq_sentences_df_long["ngrams"].value_counts()
    ).reset_index()

    unique_ngrams = ngram_counts["ngrams"]

    matches = match_to_lookup(
        unique_ngrams, lookup, sent_model, 0
    )  # setting threshold to 0 - get any match (even bad ones)

    matches = pd.merge(matches, ngram_counts, on="ngrams", how="left")

    logging.info(matches["count"].describe())

    jq_sentences_df_long = pd.merge(
        jq_sentences_df_long, matches, how="left", left_on="ngrams", right_on="ngrams"
    )

    save_to_s3(
        BUCKET_NAME,
        jq_sentences_df_long,
        f"job_quality/early_years/jq_sentences/jq_sentences_2023_matched_{today}.parquet",
    )
    save_to_s3(
        BUCKET_NAME,
        matches,
        f"job_quality/early_years/jq_sentences/unique_ngrams_2023_matched_{today}.parquet",
    )
