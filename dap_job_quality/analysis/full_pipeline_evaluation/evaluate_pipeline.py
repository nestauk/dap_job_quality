from dap_job_quality.pipeline.find_job_quality import JobQuality

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
)

import pandas as pd
from tqdm import tqdm

from datetime import datetime

mapping_evaluation_dir = "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/mapping_evaluation"

evaluation_results_dir = "s3://open-jobs-lake/job_quality/outputs/evaluation"

evaluation_file_name = "evaluation_data_12_08_24_per_sentence_evaluation.csv"

jq_cols = [
    "L&D",
    "CAREER",
    "HOURS",
    "FLEX_HOURS",
    "SHIFT",
    "LOC",
    "FLEX_LOC",
    "CONTRACT",
    "LEAVE",
    "COMP",
    "PERKS",
    "CARING",
    "DISABILITY",
    "HEALTH",
    "M_HEALTH",
    "SPONSORSHIP",
    "REWARD",
    "MISC",
    "AUTONOMY",
    "SENSE OF PURPOSE",
    "SOCIAL",
    "VOICE REPRESENTATION",
]


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


def load_evaluation_data(subcategory_to_dimension: dict) -> pd.DataFrame:
    """
    Load the evaluation dataset and filter for any job adverts not labelled.
    """

    eval_data = pd.read_csv(f"{evaluation_results_dir}/{evaluation_file_name}")
    eval_data = eval_data[pd.notnull(eval_data["Labelled by?"])].reset_index(drop=True)

    return eval_data


def get_error_type(truth, pred):
    if truth == True:
        if pred == False:
            return "FN"
        else:
            return "TP"
    else:
        if pred == False:
            return "TN"
        else:
            return "FP"


if __name__ == "__main__":

    tp_to_subcategory, tp_to_dimension, subcategory_to_dimension = get_keyword_lookups()

    eval_data = load_evaluation_data(subcategory_to_dimension)

    per_job_ad = eval_data.groupby("id")[jq_cols].sum().reset_index()

    per_job_ad["all_jq"] = per_job_ad[jq_cols].apply(
        lambda x: list(set([jq_name for jq_name in jq_cols if x[jq_name] != 0])), axis=1
    )

    job_ad_desc = dict(zip(eval_data["id"], eval_data["description"]))

    # Get the true labels in a dict format per sentence
    true_labels = {}
    for job_ad_id, job_ad_labels in eval_data.dropna(how="all", subset=jq_cols).groupby(
        "id"
    ):
        true_labels[job_ad_id] = (
            job_ad_labels.groupby("sentences")
            .apply(lambda x: x[jq_cols].any()[x[jq_cols].any()].index.tolist())
            .to_dict()
        )

    # Predict job quality for evaluation dataset

    job_quality = JobQuality()
    job_quality.load()

    eval_job_adverts = (
        eval_data[["id", "description"]].drop_duplicates().reset_index(drop=True)
    )

    jq_df_filtered, job_id_to_target_phrase = job_quality.extract_job_quality(
        eval_job_adverts, "id", "description"
    )
    jq_predictions = jq_df_filtered[["id", "target_phrase", "cosine_similarity"]]

    jq_predictions["subcategory"] = jq_predictions["target_phrase"].map(
        tp_to_subcategory
    )
    jq_predictions["dimension"] = jq_predictions["target_phrase"].map(tp_to_dimension)

    per_job_ad_preds = (
        pd.get_dummies(
            jq_predictions[["id", "subcategory"]],
            columns=["subcategory"],
            prefix="prediction",
        )
        .groupby("id")
        .sum()
        .reset_index()
    )

    comparison_df = per_job_ad.merge(per_job_ad_preds, on="id", how="left").fillna(
        value=0
    )

    comparison_df_binary = comparison_df != 0
    comparison_df_binary["id"] = comparison_df["id"]

    class_rep_per_jq = {}
    for jq_measure in jq_cols:
        if f"prediction_{jq_measure}" in comparison_df_binary:
            pred_list = comparison_df_binary[f"prediction_{jq_measure}"]
        else:
            pred_list = [False] * len(comparison_df_binary)

        class_rep = classification_report(
            comparison_df_binary[jq_measure], pred_list, output_dict=True
        )
        class_rep_per_jq[jq_measure] = class_rep

    eval_results_df = pd.DataFrame(class_rep_per_jq).T.reset_index(
        names="JQ_measure_name"
    )

    # When are the predictions incorrect?
    # Get the prediction sentences
    jq_df_filtered["ngrams_with_cat"] = jq_df_filtered.apply(
        lambda x: (
            x["ngrams"],
            x["cosine_similarity"],
            x["target_phrase"],
            tp_to_subcategory[x["target_phrase"]],
        ),
        axis=1,
    )
    exact_pred_ngrams = {
        k: list(v)
        for k, v in jq_df_filtered.groupby("id")["ngrams_with_cat"]
        .unique()
        .to_dict()
        .items()
    }

    jq_incorrect_dict = {}
    for jq_measure in jq_cols:
        jq_incorrect_dict[jq_measure] = comparison_df_binary.apply(
            lambda x: get_error_type(x[jq_measure], x[f"prediction_{jq_measure}"]),
            axis=1,
        ).tolist()

    jq_incorrect_df = pd.DataFrame(jq_incorrect_dict)
    jq_incorrect_df["id"] = comparison_df_binary["id"]
    jq_incorrect_df["description"] = jq_incorrect_df["id"].map(job_ad_desc)
    jq_incorrect_df["true_labels"] = jq_incorrect_df["id"].map(true_labels)
    jq_incorrect_df["n_incorrect"] = (
        jq_incorrect_df[jq_cols].isin(["FN", "FP"]).sum(axis=1)
    )

    jq_incorrect_df = jq_incorrect_df.dropna(how="all", subset=jq_cols)
    jq_incorrect_df["pred_ngram_matched"] = jq_incorrect_df["id"].map(exact_pred_ngrams)

    job_ad_context = eval_data[
        [
            "id",
            "company_raw",
            "job_title_raw",
            "job_location_raw",
            "created",
            "type",
            "sector",
            "parent_sector",
            "knowledge_domain",
            "occupation",
            "itl_3_code",
            "itl_3_name",
        ]
    ].drop_duplicates()
    jq_incorrect_df = jq_incorrect_df.merge(job_ad_context, on="id")

    # Save results
    TODAY = datetime.today().strftime("%Y-%m-%d")

    eval_results_df.to_csv(
        f"{evaluation_results_dir}/JQ_evaluation_results_{TODAY}.csv"
    )

    jq_incorrect_df.to_csv(f"{evaluation_results_dir}/JQ_prediction_errors_{TODAY}.csv")
