from dap_job_quality.getters.train_val_test import get_df
from dap_job_quality.pipeline.find_job_quality import JobQuality

import pandas as pd
from tqdm import tqdm

from datetime import datetime

mapping_evaluation_dir = "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/mapping_evaluation"

evaluation_results_dir = "s3://open-jobs-lake/job_quality/outputs/evaluation"


def get_keyword_lookups():
    """
    Use the keyword lookup table to create mappings between the JQ taxonomy levels
    """

    keyword_lookup_data = pd.read_csv(
        f"{mapping_evaluation_dir}/keyword_lookup_v7_080824.csv"
    )
    tp_to_subcategory = dict(
        zip(keyword_lookup_data["target_phrase"], keyword_lookup_data["subcategory"])
    )
    tp_to_dimension = dict(
        zip(keyword_lookup_data["target_phrase"], keyword_lookup_data["dimension"])
    )
    subcategory_to_dimension = dict(
        zip(keyword_lookup_data["subcategory"], keyword_lookup_data["dimension"])
    )

    return tp_to_subcategory, tp_to_dimension, subcategory_to_dimension


def combine_evaluation_data(subcategory_to_dimension: dict) -> pd.DataFrame:
    """
    Load the files needed for creating an evaluation dataset, process and merge them together.
    Remove any job adverts which were also in the JQ sentence classifier training or validation.
    """

    train_df = get_df("train")
    val_df = get_df("val")

    # Sentences with spans found, then these checked
    eval_data = pd.read_csv(
        f"{mapping_evaluation_dir}/sample_10000_evaluation_080824.csv"
    )
    eval_data = eval_data[eval_data["rating"] == "good"].reset_index()

    # Full job ads with which type of contract was found
    contract_data = pd.read_csv(
        f"{mapping_evaluation_dir}/contract_type_sample_080824.csv"
    )
    contract_data = contract_data[
        contract_data["Contract_type_quality?"].isin(
            ["Temporary", "Contract", "Permanent"]
        )
    ].reset_index(drop=True)
    contract_data.rename(
        columns={"Contract_type_quality?": "target_phrase"}, inplace=True
    )
    contract_data["subcategory"] = "CONTRACT"
    contract_data["dimension"] = subcategory_to_dimension["CONTRACT"]

    # Join together
    eval_data = pd.concat(
        [eval_data.rename(columns={"clean_description": "description"}), contract_data]
    ).reset_index(drop=True)

    # Check all job adverts in the training and validation of the JQ sentence classifier aren't included
    train_val_ids = set(train_df["id"].tolist() + val_df["id"].tolist())
    eval_data = eval_data[~eval_data["id"].isin(train_val_ids)]

    return eval_data


if __name__ == "__main__":

    tp_to_subcategory, tp_to_dimension, subcategory_to_dimension = get_keyword_lookups()

    eval_data = combine_evaluation_data(subcategory_to_dimension)

    # Predict job quality for evaluation dataset

    job_quality = JobQuality()
    job_quality.load()

    eval_job_adverts = (
        eval_data[["id", "description"]].drop_duplicates().reset_index(drop=True)
    )

    chunk_size = 50
    job_ad_chunks = [
        eval_job_adverts.iloc[i : i + chunk_size]
        for i in range(0, len(eval_job_adverts), chunk_size)
    ]
    jq_predictions = pd.DataFrame()
    for i, job_chunk in tqdm(enumerate(job_ad_chunks)):
        jq_df_filtered_chunk, _ = job_quality.extract_job_quality(
            job_chunk, "id", "description"
        )
        chunk_predictions = jq_df_filtered_chunk[
            ["id", "target_phrase", "cosine_similarity"]
        ]

        if len(jq_predictions) == 0:
            jq_predictions = chunk_predictions
        else:
            jq_predictions = pd.concat([jq_predictions, chunk_predictions])

    jq_predictions["subcategory"] = jq_predictions["target_phrase"].map(
        tp_to_subcategory
    )
    jq_predictions["dimension"] = jq_predictions["target_phrase"].map(tp_to_dimension)

    # The proportion of job adverts we know to contain a particular JQ measure
    # which had this measure predicted too.
    eval_results = {}
    for jq_level in ["dimension", "subcategory", "target_phrase"]:
        for jq_measure in eval_data[jq_level].unique():
            # The job advert ids which have this measure (truth)
            truth_ids = set(eval_data[eval_data[jq_level] == jq_measure]["id"])
            # The job advert ids predicted to have this measure
            pred_ids = set(jq_predictions[jq_predictions[jq_level] == jq_measure]["id"])
            eval_results[jq_measure] = {
                "JQ_level": jq_level,
                # The proportion of job ads which have this measure which were also predicted to have it
                "prop_ads_truth_and_pred": len(truth_ids.intersection(pred_ids))
                / len(truth_ids),
                "n_ads_truth": len(truth_ids),
                "n_ads_pred": len(pred_ids),
                # The proportion of all job ads predicted to have this measure
                "prop_ads_pred": len(pred_ids) / jq_predictions["id"].nunique(),
            }

    eval_results_df = pd.DataFrame(eval_results).T.reset_index(names="JQ_measure_name")

    # Save results
    TODAY = datetime.today().strftime("%Y-%m-%d")

    jq_predictions.merge(eval_job_adverts, on="id", how="left").to_csv(
        f"{evaluation_results_dir}/JQ_predictions_{TODAY}.csv"
    )

    eval_results_df.to_csv(f"{evaluation_results_dir}/recall_results_{TODAY}.csv")
