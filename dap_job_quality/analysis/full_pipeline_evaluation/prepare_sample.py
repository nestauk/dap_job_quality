from dap_job_quality.getters.ojo_getters import get_ojo_job_title_sample, get_ojo_sample
from dap_job_quality.utils.text_cleaning import clean_text

import nltk
from nltk.tokenize import sent_tokenize
import pandas as pd

if __name__ == "__main__":

    # Load the sentence classifier test data, plus the sector and job descriptions data
    test_data = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/test_df_20240725.parquet"
    )
    ojo_sector_data = get_ojo_job_title_sample()
    ojo_desc = get_ojo_sample()
    ojo_sample = ojo_sector_data.merge(
        ojo_desc[["id", "description", "itl_3_code", "itl_3_name"]], on="id", how="left"
    )

    # Get some more data for key EY careers (this is underrepresented in the sentence classifier test dataset)
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

    # Merge together
    eval_data = ojo_sample[ojo_sample["id"].isin(test_data["id"])]
    eval_data["origin"] = "sent_class_test"
    eval_data = pd.concat([eval_data, extra_eyp])

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
        "s3://open-jobs-lake/job_quality/outputs/evaluation/evaluation_data.csv",
        index=False,
    )
