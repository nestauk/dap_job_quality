from datetime import datetime
import pandas as pd

from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.getters.keywords import get_keywords

TODAY = datetime.today().strftime("%Y-%m-%d")

if __name__ == "__main__":
    # The data was produced by 01_concat_data.py ...
    # ... and manually labelled here:
    # https://docs.google.com/spreadsheets/d/1bXNmO9vOLG6zdDpHl0Tdw9AeDqb43CrpkXzNGyHRhWI/edit?gid=4769099#gid=4769099
    positive_sents_manually_labelled = pd.read_csv(
        "s3://open-jobs-lake/job_quality/sentence_classifier/inputs/labelled/positive_sents_for_labelling - positive_sents_for_labelling.csv"
    )

    category_representation = pd.DataFrame(
        positive_sents_manually_labelled["subcategory"].value_counts(normalize=True)
    ).reset_index()
    category_representation["perc"] = round(category_representation["proportion"] * 100)

    keywords = get_keywords()

    category_representation = pd.merge(
        category_representation,
        keywords[["dimension", "subcategory"]].drop_duplicates(),
        on="subcategory",
        how="outer",
    )

    save_to_s3(
        BUCKET_NAME,
        category_representation,
        f"job_quality/sentence_classifier/inputs/labelled/category_representation_{TODAY}.csv",
    )
