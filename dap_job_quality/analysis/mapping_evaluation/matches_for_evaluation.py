import pandas as pd

from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.keywords import get_keywords
from dap_job_quality.getters.data_getters import load_s3_data, save_to_s3

LOOKUP = get_keywords()


def get_sample(df, n=100, seed=42):
    """
    Assuming your df has multiple rows per 'id' and 'sentences_split',
    this function will return a sample of n sentences (rather than sampling rows).

    Args:
        df (_type_): _description_
        n (int, optional): _description_. Defaults to 100.
        seed (int, optional): _description_. Defaults to 42.

    Returns:
        _type_: _description_
    """
    sample_sents = (
        df[["id", "sentences_split"]].drop_duplicates().sample(n, random_state=seed)
    )
    sample_sents = sample_sents.merge(df, on=["id", "sentences_split"], how="left")
    return sample_sents


if __name__ == "__main__":
    # Data produced by dap_job_quality/pipeline/find_job_quality.py
    eval_data = load_s3_data(
        BUCKET_NAME,
        "job_quality/early_years/evaluation_sample/job_ads_prod_True_sample_10000_2024-06-20.parquet",
    )

    eval_data = eval_data.merge(LOOKUP, on="target_phrase", how="left")

    save_to_s3(
        BUCKET_NAME,
        eval_data,
        "job_quality/early_years/evaluation_sample/sample_10000_evaluation.csv",
    )

    # Extract a subsample of sentences to evaluate
    target_dimensions = ["FLEX_LOC", "FLEX_HOURS", "HOURS"]

    matched_to_target = eval_data[eval_data["subcategory"].isin(target_dimensions)]
    not_matched_to_target = eval_data[~eval_data["subcategory"].isin(target_dimensions)]

    matched_to_target_100 = get_sample(matched_to_target)
    not_matched_to_target_100 = get_sample(not_matched_to_target)

    evaluation_sentences = pd.concat(
        [matched_to_target_100, not_matched_to_target_100]
    )[
        [
            "id",
            "sentences_split",
            "clean_description",
            "ngrams",
            "target_phrase",
            "subcategory",
        ]
    ]

    # Create binary columns for each unique value of 'subcategory'
    subcategory_dummies = pd.get_dummies(evaluation_sentences["subcategory"])

    # Add the suffix '_pred' to each column name - we will manually add '_true' columns when labelling
    subcategory_dummies = subcategory_dummies.add_suffix("_pred")
    # convert bool to int
    subcategory_dummies = subcategory_dummies.astype(int)

    df_with_dummies = pd.concat([evaluation_sentences, subcategory_dummies], axis=1)

    # Pivot from long to wide: squash 'ngrams' and 'target_phrase' into lists so that we
    # end up with one row per sentence
    grouped_df = (
        df_with_dummies.groupby(["id", "clean_description", "sentences_split"])
        .agg(
            {
                "ngrams": list,
                "target_phrase": list,
                **{col: "max" for col in subcategory_dummies.columns},
            }
        )
        .reset_index()
    )

    save_to_s3(
        BUCKET_NAME,
        grouped_df,
        "job_quality/early_years/evaluation_sample/evaluation_sentences.csv",
    )
