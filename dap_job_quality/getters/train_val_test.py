import pandas as pd

from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.data_getters import load_s3_data


def get_df(set: str = "train") -> pd.DataFrame:
    """Load the training/val/test data.

    Args:
        set (str, optional): Must be "train", "val" or "test". Defaults to "train".

    Returns:
        pd.DataFrame: DataFrame with the columns 'id', 'sentence', 'label'.
        'label' is binary: 0 if the sentence is not related to job quality, 1 if it is.
    """
    df = load_s3_data(
        BUCKET_NAME,
        f"job_quality/sentence_classifier/inputs/labelled/{set}_df_20240725.parquet",
    )
    return df


def get_ids(set: str = "train") -> pd.DataFrame:
    """Get the unique IDs of job adverts in the train/val/test set.

    Args:
        set (str, optional): Must be "train", "val" or "test". Defaults to "train".

    Returns:
        pd.DataFrame: Dataframe with the columns 'id' and 'majority_label'.
        'majority_label' is the label that most sentences in that advert have ie 1= the majority
        of sentences are about job quality; 0 = the majority of sentences are not about job quality.
    """
    ids = load_s3_data(
        BUCKET_NAME,
        f"job_quality/sentence_classifier/inputs/labelled/{set}_ids_20240725.parquet",
    )
    return ids
