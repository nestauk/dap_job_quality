from dap_job_quality.getters.ojo_getters import get_ojo_job_title_sample, get_ojo_sample
from dap_job_quality.utils.text_cleaning import clean_text
from dap_job_quality import logging

import nltk
from nltk.tokenize import sent_tokenize
import pandas as pd

if __name__ == "__main__":

    # Load the sentence classifier test data, plus the sector and job descriptions data
    val_ids = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/val_ids_20240725.parquet"
    )
    train_ids = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/train_ids_20240725.parquet"
    )
    train_val = pd.concat(
        [train_ids.drop_duplicates("id"), val_ids.drop_duplicates("id")]
    )

    ojo_sector_data = get_ojo_job_title_sample()
    ojo_desc = get_ojo_sample()
    ojo_sample = ojo_sector_data.merge(
        ojo_desc[["id", "description", "itl_3_code", "itl_3_name"]], on="id", how="left"
    )

    extra_eyp = (
        ojo_sample[
            ojo_sample["sector"].isin(
                [
                    "Primary School",
                    "Waiting &amp; Bar Staff",
                    "Other Retail",
                    "Sales Assistant",
                ]
            )
        ]
        .groupby("sector")
        .sample(20, random_state=42)
        .reset_index(drop=True)
    )
    extra_eyp["origin"] = "extra_eyp"

    eval_data = ojo_sample[ojo_sample["id"].isin(train_val["id"])]
    eval_data["origin"] = "sent_class_val"
    eval_data = pd.concat([eval_data, extra_eyp])

    # Sample so that you get a maximum of 5 ads per parent_sector
    sampled_df = eval_data.groupby("parent_sector", group_keys=False).apply(
        lambda x: x.sample(min(len(x), 5))
    )

    sampled_df = sampled_df.reset_index(drop=True)
    logging.info(f"Number of ads in sample: {len(sampled_df)}")

    # Separate by sentence
    eval_data["clean_description"] = (
        eval_data["description"]
        .apply(clean_text)
        .str.replace("[", "")
        .str.replace("]", "")
        .str.strip()
    )
    eval_data["sentences"] = eval_data["clean_description"].apply(
        lambda x: sent_tokenize(x)
    )
    eval_data_per_sentence = eval_data.explode("sentences")

    # Save
    eval_data_per_sentence.to_csv(
        "s3://open-jobs-lake/job_quality/outputs/evaluation/evaluation_data_20240827.csv",
        index=False,
    )
