import nltk
from nltk.tokenize import sent_tokenize
import pandas as pd

from dap_job_quality import config, logging, BUCKET_NAME
from dap_job_quality.getters.afs_data import get_eyp_ads, get_sim_occ_ads
from dap_job_quality.getters.data_getters import save_to_s3
from dap_job_quality.utils import jobbert

nltk.download("punkt")
nltk.download("stopwords")

# Load BERT model and tokenizer
JOBBERT = config["sentence_model"]

if __name__ == "__main__":
    # Load and concat data
    eyp = get_eyp_ads()
    sim_occs = get_sim_occ_ads()
    all_job_ads = pd.concat([eyp, sim_occs], axis=0).drop_duplicates()

    logging.info(f"Total number of job ads: {len(all_job_ads)}")
    all_job_ads = all_job_ads[all_job_ads["created"] >= "2023-01-01"]
    logging.info(f"Number of job ads from 2023: {len(all_job_ads)}")
    logging.info(
        f"{len(all_job_ads[all_job_ads['clean_description']=='nan']) / len(all_job_ads)} of job ads have no description"
    )

    all_job_ads["sentences"] = all_job_ads["clean_description"].apply(
        sent_tokenize
    )  # split job descriptions into sentences
    all_job_ads = all_job_ads.explode("sentences")  # 1 row per sentence
    logging.info(f"{len(all_job_ads)} sentences")

    # Add an identifier column
    all_job_ads.reset_index(inplace=True)
    all_job_ads["identifier"] = all_job_ads["id"] + "_" + all_job_ads["index"]

    ad_embeddings = jobbert.embed_sentences(
        all_job_ads["sentences"].tolist(), JOBBERT, 64
    )

    # Numpy preserves order, whereas parquet does not
    save_to_s3(
        BUCKET_NAME,
        ad_embeddings.numpy(),
        "job_quality/early_years/embeddings_2023.npy",
    )
    save_to_s3(
        BUCKET_NAME,
        all_job_ads["identifier"].values,
        "job_quality/early_years/identifiers_2023.npy",
    )
    save_to_s3(BUCKET_NAME, all_job_ads, "job_quality/early_years/all_ads_2023.parquet")
