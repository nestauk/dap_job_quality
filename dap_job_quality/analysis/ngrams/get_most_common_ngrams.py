from collections import Counter
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import sent_tokenize
import pandas as pd

from dap_job_quality import PROJECT_DIR, logging
from dap_job_quality.utils import text_cleaning as tc
from dap_job_quality.getters.afs_data import get_eyp_ads, get_sim_occ_ads

stop_words = set(stopwords.words("english"))
lemmatizer = WordNetLemmatizer()


def get_most_common_n_grams(
    texts_list: list, top_n: int = 100, ngram_range=range(2, 6)
) -> dict:
    """
    Get top_n most common ngrams from a list of texts
    """

    # Slightly clean and combine all texts in a list into a big string
    combined_texts = ""
    for topic_text in texts_list:
        # tweet_text = re.sub(r'[^\w\s]','',topic_text).lower()
        combined_texts = combined_texts + " " + topic_text.lower()

    # Calculate most common ngrams for a few different n
    results = {}
    for n in ngram_range:
        combined_texts_list = combined_texts.split()
        combined_texts_list = [
            lemmatizer.lemmatize(word)
            for word in combined_texts_list
            if word not in stop_words
        ]
        found_grams = ngrams(combined_texts_list, n)
        all_grams = []
        for grams in found_grams:
            all_grams.append(" ".join(grams))
        results[n] = [
            (a, b, round(b / len(all_grams), 6))
            for a, b in Counter(all_grams).most_common(top_n)
        ]
    return results


if __name__ == "__main__":
    logging.info("reading EYP data")
    eyp = get_eyp_ads()
    eyp = eyp[["id", "clean_description"]]
    logging.info("reading similar occupations data")
    sim_occs = get_sim_occ_ads()
    sim_occs = sim_occs[["id", "clean_description"]]
    logging.info("Concatenating datasets")
    all_job_ads = pd.concat([eyp, sim_occs], axis=0).drop_duplicates()
    logging.info(f"Total number of job ads: {len(all_job_ads)}")
    logging.info("Tokenizing sentences")
    all_job_ads["sentences"] = all_job_ads["clean_description"].apply(sent_tokenize)

    all_job_ads_long = all_job_ads.explode("sentences")

    # I would like all sentences that contain eg 'salary £xx,xxx per annum' to be recognised
    # as one ngram, so we'll replace digits with D
    logging.info("cleaning digits")
    all_job_ads_long["sentences_cleaned"] = all_job_ads_long["sentences"].str.replace(
        r"\d", "D", regex=True
    )

    logging.info("getting most common ngrams")
    ngram_dict = get_most_common_n_grams(
        all_job_ads_long["sentences_cleaned"].tolist(), 100
    )

    for n in ngram_dict.keys():
        ngram_dict[n] = pd.DataFrame(
            ngram_dict[n], columns=["ngram", "count", "proportion"]
        )

    for n in ngram_dict.keys():
        logging.info(f"Saving ngram_{n}.csv")
        ngram_dict[n].to_csv(PROJECT_DIR / f"outputs/data/ngram_{n}.csv", index=False)
