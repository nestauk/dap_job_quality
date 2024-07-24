import ast
from nltk import sent_tokenize
import pandas as pd
from typing import List

from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.data_getters import load_s3_data, save_to_s3
from dap_job_quality.getters.labelled_data import (
    get_positive_sents_labelled_for_categories,
    get_search_terms_for_small_categories,
)
from dap_job_quality.getters.ojo_getters import get_ojo_sample
from dap_job_quality.utils import text_cleaning as tc

SENTS_PER_CAT = 50


def find_search_terms(text: str, search_terms_list: List[str]) -> List[str]:
    """Find search terms in text.

    Args:
        text (str): The input text.
        search_terms_list (List[str]): List of search terms.

    Returns:
        List[str]: List of search terms found in the text.
    """
    found_terms = [term for term in search_terms_list if term in text]
    return found_terms


if __name__ == "__main__":
    ids_already_labelled = (
        get_positive_sents_labelled_for_categories()["id"].unique().tolist()
    )

    categories = get_search_terms_for_small_categories()
    categories = categories.dropna(subset=["search_terms"])
    categories["search_terms"] = categories["search_terms"].apply(ast.literal_eval)
    categories = categories.explode("search_terms")

    search_terms_list = categories["search_terms"].unique().tolist()

    ojo_sample = get_ojo_sample()

    ojo_sample = ojo_sample[~ojo_sample["id"].isin(ids_already_labelled)]

    ojo_sample["description_cleaned"] = (
        ojo_sample["description"]
        .apply(tc.clean_text)
        .str.replace("[", "")
        .str.replace("]", "")
        .str.strip()
    )

    ojo_sample["sentences"] = ojo_sample["description_cleaned"].apply(sent_tokenize)

    ojo_sample_long = ojo_sample.explode("sentences")

    ojo_sample_long["search_terms"] = ojo_sample_long["sentences"].apply(
        lambda text: find_search_terms(text, search_terms_list)
    )

    ojo_sample_filtered = ojo_sample_long[
        ojo_sample_long["search_terms"].apply(bool)
    ].reset_index(drop=True)

    ojo_sample_filtered_long = ojo_sample_filtered.explode("search_terms")
    ojo_sample_filtered_long = pd.merge(
        ojo_sample_filtered_long,
        categories[["search_terms", "subcategory"]],
        on="search_terms",
        how="left",
    )

    sampled_ojo = (
        ojo_sample_filtered_long.groupby("subcategory")
        .apply(lambda x: x.sample(min(len(x), SENTS_PER_CAT), random_state=42))
        .reset_index(drop=True)
    )

    sampled_ojo_deduplicated = sampled_ojo.drop_duplicates(subset=["id", "sentences"])

    save_to_s3(
        BUCKET_NAME,
        sampled_ojo_deduplicated,
        "job_quality/sentence_classifier/inputs/labelling/additional_green_jobs_examples_20240704.csv",
    )
