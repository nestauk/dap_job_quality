from collections import Counter
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import pandas as pd

from dap_job_quality import BUCKET_NAME, PROJECT_DIR
from dap_job_quality.getters.data_getters import load_s3_jsonl
from dap_job_quality.utils import prodigy_data_utils as pdu
from dap_job_quality.utils import text_cleaning as tc

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


def get_clean_sentences():
    labelled_sents1 = load_s3_jsonl(
        BUCKET_NAME,
        s3_file_name="job_quality/prodigy/binary_classifier_labelled_data/20240416/job_sentences_labelled_20240416.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/job_sentences_labelled_20240509.jsonl",
    )[0][10:]

    labelled_sents = load_s3_jsonl(
        BUCKET_NAME,
        s3_file_name="job_quality/prodigy/labelled_data/job_sentences_labelled_20240528.jsonl",
        local_file=PROJECT_DIR
        / f"inputs/labelled/job_sentences_labelled_20240528.jsonl",
    )

    unique_ids = []

    for ad in labelled_sents1:
        if ad["id"] not in unique_ids:
            unique_ids.append(ad["id"])

    for ad in labelled_sents:
        if ad["meta"]["id"] not in unique_ids:
            unique_ids.append(ad["meta"]["id"])

    labelled_data = pdu.get_spans_and_sentences(labelled_sents, chunks=True)
    labelled_data1 = pdu.get_spans_and_sentences(labelled_sents1, chunks=False)

    labelled_df = pd.DataFrame(columns=["span", "sent", "text", "job_id", "chunk"])

    for job in labelled_data.keys():
        for chunk in labelled_data[job].keys():
            temp_df = pd.DataFrame(labelled_data[job][chunk])
            temp_df["job_id"] = job
            temp_df["chunk"] = chunk
            labelled_df = pd.concat([labelled_df, temp_df])

    for job in labelled_data1.keys():
        temp_df = pd.DataFrame(labelled_data1[job])
        temp_df["job_id"] = job
        temp_df["chunk"] = None
        labelled_df = pd.concat([labelled_df, temp_df])

    labelled_df_clean = labelled_df[labelled_df["span"] != ""]

    labelled_df_clean["sentence"] = labelled_df_clean["sent"].apply(lambda x: x.text)

    return labelled_df_clean


if __name__ == "__main__":
    labelled_df_clean = get_clean_sentences()

    # get just the unique JQ sentences
    sentence_df = labelled_df_clean[["job_id", "sentence"]].drop_duplicates()
    # In case some sentences were not split correctly, split these up
    sentence_df["sentences"] = sentence_df["sentence"].apply(tc.split_sentences)

    sentence_df_long = sentence_df.explode("sentences")

    # After the splitting, remove any empty sentences
    sentence_df_long = sentence_df_long[
        ~sentence_df_long["sentences"].isin(["", " ", ".", "!"])
    ]

    # I would like all sentences that contain eg 'salary £xx,xxx per annum' to be recognised
    # as one ngram, so we'll replace digits with D
    sentence_df_long["sentences_cleaned"] = sentence_df_long["sentences"].str.replace(
        r"\d", "D", regex=True
    )

    ngram_dict = get_most_common_n_grams(
        sentence_df_long["sentences_cleaned"].tolist(), 100
    )

    for n in ngram_dict.keys():
        ngram_dict[n] = pd.DataFrame(
            ngram_dict[n], columns=["ngram", "count", "proportion"]
        )

    for n in ngram_dict.keys():
        ngram_dict[n].to_csv(PROJECT_DIR / f"outputs/data/ngram_{n}.csv", index=False)
