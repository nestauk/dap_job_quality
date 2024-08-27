from dap_job_quality.pipeline.find_job_quality import JobQuality
from dap_job_quality.getters.keywords import get_keywords

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
from typing import Tuple

mapping_evaluation_dir = "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/mapping_evaluation"

evaluation_results_dir = "s3://open-jobs-lake/job_quality/outputs/evaluation"

evaluation_file_name = "evaluation_data_12_08_24_per_sentence_evaluation_19_08_24.csv"

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

    keyword_lookup_data = get_keywords()

    tp_to_subcategory = dict(
        zip(keyword_lookup_data["target_phrase"], keyword_lookup_data["subcategory"])
    )

    return tp_to_subcategory


def load_evaluation_data() -> pd.DataFrame:
    """
    Load the evaluation dataset and filter for any job adverts not labelled.
    """

    eval_data = pd.read_csv(f"{evaluation_results_dir}/{evaluation_file_name}")
    eval_data = eval_data[pd.notnull(eval_data["Labelled by?"])].reset_index(drop=True)

    return eval_data


def predict_evaluation_data(
    eval_data: pd.DataFrame, tp_to_subcategory: dict
) -> Tuple[pd.DataFrame, dict]:
    """
    Predict JQ measures for the job adverts in the evaluation dataset
    Arguments:
            eval_data (pd.DataFrame): The evaluation data with an id and a description column.
            tp_to_subcategory (dict): A dictionary mapping the target phrases to the subcategory.

    Returns:
            pd.DataFrame: A matrix of the counts of each predicted JQ measures (columns) per job id (rows).
            dict: A dictionary with information about the predictions per job id (keys)
                    e.g. {job_id: [(ngram, cosine similarity, target phrase, subcategory), ...]}
    """

    job_quality = JobQuality()
    job_quality.load()

    jq_predictions_df, _ = job_quality.extract_job_quality(
        eval_data[["id", "description"]].drop_duplicates().reset_index(drop=True),
        "id",
        "description",
    )

    jq_predictions_df["subcategory"] = jq_predictions_df["target_phrase"].map(
        tp_to_subcategory
    )

    # Get a matrix of predicted JQ measures (columns, binary) per job id (rows)
    jq_predictions_per_id = (
        pd.get_dummies(
            jq_predictions_df[["id", "subcategory"]],
            columns=["subcategory"],
            prefix="prediction",
        )
        .groupby("id")
        .sum()
        .reset_index()
    )

    # Get the prediction information
    jq_predictions_df["ngrams_with_cat"] = jq_predictions_df.apply(
        lambda x: (
            x["ngrams"],
            x["cosine_similarity"],
            x["target_phrase"],
            tp_to_subcategory[x["target_phrase"]],
        ),
        axis=1,
    )
    pred_ngrams_per_id = {
        k: list(v)
        for k, v in jq_predictions_df.groupby("id")["ngrams_with_cat"]
        .unique()
        .to_dict()
        .items()
    }

    return jq_predictions_per_id, pred_ngrams_per_id


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


def format_to_explore_errors_data(
    comparison_df: pd.DataFrame, eval_data: pd.DataFrame, pred_ngrams_per_id: dict
) -> pd.DataFrame:
    """
    Get data in format useful to explore errors
    Arguments:
            comparison_df (pd.DataFrame): Both truth and predicted results of whether a job advert contains each JQ quality measure
            eval_data (pd.DataFrame): The original evaluation data - used to get the extra information about the job advert (e.g. sector)
            pred_ngrams_per_id (dict): The ngrams extracted and predicted JQ measures per job ad

    Returns:
            pd.DataFrame: A dataframe where each row is a job advert - the error type for each JQ measure is given, the true and predicted
                    sentences/ngrams which contained JQ measures, and job advert information (e.g. description and sector)
    """

    # Get the error types per job id for each JQ measure
    jq_error_types_dict = {}
    for jq_measure in jq_cols:
        jq_error_types_dict[jq_measure] = comparison_df.apply(
            lambda x: get_error_type(x[jq_measure], x[f"prediction_{jq_measure}"]),
            axis=1,
        ).tolist()
    jq_error_analysis_df = pd.DataFrame(jq_error_types_dict)
    jq_error_analysis_df["id"] = comparison_df["id"]
    jq_error_analysis_df["n_incorrect"] = (
        jq_error_analysis_df[jq_cols].isin(["FN", "FP"]).sum(axis=1)
    )

    # Get the sentences and JQ measures per job advert {job_id: {sentence: [JQ measures]}}
    true_jq_sents_per_id = {}
    for job_ad_id, job_ad_labels in eval_data.dropna(how="all", subset=jq_cols).groupby(
        "id"
    ):
        true_jq_sents_per_id[job_ad_id] = (
            job_ad_labels.groupby("sentences")
            .apply(lambda x: x[jq_cols].any()[x[jq_cols].any()].index.tolist())
            .to_dict()
        )

    jq_error_analysis_df["jq_sentences"] = jq_error_analysis_df["id"].map(
        true_jq_sents_per_id
    )

    jq_error_analysis_df["pred_ngram_matched"] = jq_error_analysis_df["id"].map(
        pred_ngrams_per_id
    )

    # Add extra information about this job advert
    job_ad_context = eval_data[
        [
            "id",
            "description",
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
    jq_error_analysis_df = jq_error_analysis_df.merge(job_ad_context, on="id")

    return jq_error_analysis_df


if __name__ == "__main__":

    tp_to_subcategory = get_keyword_lookups()

    eval_data = load_evaluation_data()

    # Predict JQ measures per job advert
    jq_predictions_per_id, pred_ngrams_per_id = predict_evaluation_data(
        eval_data, tp_to_subcategory
    )

    # Get the true JQ measures per job advert
    jq_truth_per_id = eval_data.groupby("id")[jq_cols].sum().reset_index()

    # Evaluate truth vs predictions

    # Merge true and predicted counts together and binarise (JQ present or not)
    comparison_counts = jq_truth_per_id.merge(
        jq_predictions_per_id, on="id", how="left"
    ).fillna(value=0)
    comparison_df = comparison_counts != 0
    comparison_df["id"] = comparison_counts["id"]

    # Get classification report for each JQ measure
    class_rep_per_jq = {}
    for jq_measure in jq_cols:
        if f"prediction_{jq_measure}" in comparison_df:
            pred_list = comparison_df[f"prediction_{jq_measure}"]
        else:
            pred_list = [False] * len(comparison_df)
        class_rep = classification_report(
            comparison_df[jq_measure], pred_list, output_dict=True
        )
        class_rep_per_jq[jq_measure] = class_rep

    eval_results_df = pd.DataFrame(class_rep_per_jq).T.reset_index(
        names="JQ_measure_name"
    )

    jq_error_analysis_df = format_to_explore_errors_data(
        comparison_df, eval_data, pred_ngrams_per_id
    )

    # Save results
    TODAY = datetime.today().strftime("%Y-%m-%d")

    eval_results_df.to_csv(
        f"{evaluation_results_dir}/JQ_evaluation_results_{TODAY}_diff_thresholds.csv"
    )

    jq_error_analysis_df.to_csv(
        f"{evaluation_results_dir}/JQ_prediction_errors_{TODAY}_diff_thresholds.csv"
    )
