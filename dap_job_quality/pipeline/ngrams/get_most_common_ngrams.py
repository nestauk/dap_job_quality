from collections import Counter
import nltk
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import sent_tokenize
import pandas as pd

from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logging
from dap_job_quality.getters.data_getters import load_s3_jsonl
from dap_job_quality.getters.afs_data import get_eyp_ads, get_sim_occ_ads
from dap_job_quality.utils import prodigy_data_utils as pdu
from dap_job_quality.utils import text_cleaning as tc

nltk.download("punkt")
nltk.download("stopwords")

stop_words = set(stopwords.words("english"))
lemmatizer = WordNetLemmatizer()

SAMPLE_SIZE = "all"
TOP_N = 200


def get_most_common_n_grams(
    texts_list: list, top_n: int = TOP_N, ngram_range=range(2, 7)
) -> dict:
    """
    Get top_n most common ngrams from a list of texts
    """
    results = {n: Counter() for n in ngram_range}

    # Process each text one by one
    for topic_text in texts_list:
        topic_text = topic_text.lower()
        combined_texts_list = topic_text.split()
        combined_texts_list = [
            lemmatizer.lemmatize(word)
            for word in combined_texts_list  # if word not in stop_words
        ]

        # Generate n-grams and update counters
        for n in ngram_range:
            found_grams = ngrams(combined_texts_list, n)
            results[n].update(" ".join(grams) for grams in found_grams)

    # Get the most common n-grams
    final_results = {}
    for n in ngram_range:
        total_ngrams = sum(results[n].values())
        final_results[n] = [
            (a, b, round(b / total_ngrams, 3)) for a, b in results[n].most_common(top_n)
        ]

    return final_results


if __name__ == "__main__":
    eyp = get_eyp_ads()
    sim_occs = get_sim_occ_ads()
    all_job_ads = pd.concat([eyp, sim_occs], axis=0).drop_duplicates()

    logging.info(len(all_job_ads))
    all_job_ads = all_job_ads[all_job_ads["created"] >= "2023-01-01"]  # .sample(
    #     SAMPLE_SIZE, random_state=42
    # )
    logging.info(len(all_job_ads))

    all_job_ads["sentences"] = all_job_ads["clean_description"].apply(sent_tokenize)
    all_job_ads = all_job_ads.explode("sentences")
    logging.info(f"{len(all_job_ads)} sentences")

    # I would like all sentences that contain eg 'salary £xx,xxx per annum' to be recognised
    # as one ngram, so we'll replace digits with D
    logging.info("Replacing digits with D...")
    all_job_ads["sentences_cleaned"] = all_job_ads["sentences"].str.replace(
        r"\d", "D", regex=True
    )

    ngram_dict = get_most_common_n_grams(
        all_job_ads["sentences_cleaned"].tolist(), TOP_N
    )

    for n in ngram_dict.keys():
        ngram_dict[n] = pd.DataFrame(
            ngram_dict[n], columns=["ngram", "count", "proportion"]
        )

    for n in ngram_dict.keys():
        ngram_dict[n].to_csv(
            PROJECT_DIR
            / f"outputs/data/ngram_{n}_top_{TOP_N}_2023_sample_size_{SAMPLE_SIZE}.csv",
            index=False,
        )
