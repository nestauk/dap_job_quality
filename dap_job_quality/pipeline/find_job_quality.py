"""
Pipeline to extract job quality measures from job adverts


The class in this script can be used by running:


from dap_job_quality.pipeline.find_job_quality import JobQuality
import pandas as pd

job_quality = JobQuality()
job_quality.load()

job_adverts = pd.DataFrame(
    [
        {'id': 123, 'description': '[This is a job adverts. It has many benefits such as a pension scheme and a cycle to work scheme.]'},
        {'id': 234, 'description': '[This is a job adverts for a job at a bank. There are free childcare vouchers. We also offer a yearly bonus and generous salary.]'}
        ]
)

jq_df_filtered, job_id_to_target_phrase = job_quality.extract_job_quality(job_adverts,
    "id",
    "description",)


This should give:
job_id_to_target_phrase = {123: ['Cycle to work', 'benefits', 'pension', 'pension scheme'], 234: ['childcare vouchers', 'compensation', 'performance bonus']}
"""

import argparse
from datasets import Dataset
from datetime import datetime
import nltk
from nltk import ngrams
from nltk.tokenize import sent_tokenize
import numpy as np
import pandas as pd
import re
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import time
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from typing import List, Union, Tuple
import time

from dap_job_quality import logging, BUCKET_NAME, PROJECT_DIR, get_yaml_config
from dap_job_quality.getters.afs_data import get_stratified_sample
from dap_job_quality.getters.data_getters import (
    save_to_s3,
    get_s3_data_paths,
    load_s3_data,
)
from dap_job_quality.getters.ojo_getters import get_ojo_sample
from dap_job_quality.getters.jobbert_jq import get_jobbert_jq
from dap_job_quality.getters.keywords import get_keywords
from dap_job_quality.utils.text_cleaning import clean_text

# from dap_job_quality.getters.data_getters import download_and_extract_from_s3

jobbert_config = get_yaml_config(
    PROJECT_DIR / "dap_job_quality/config/jobbert_config.yaml"
)

print(jobbert_config)


def split_ngrams(
    text: str, length: int = 6, n: int = 4
) -> Union[List[Tuple[str, ...]], List[str]]:
    """
    Split the input text into n-grams if the number of words in the text exceeds the specified length.

    Args:
        text (str): The input text to be split into n-grams.
        length (int): The maximum length of text in words before splitting into n-grams. Default is 6.
        n (int): The number of words in each n-gram. Default is 4.

    Returns:
        Union[List[Tuple[str, ...]], List[str]]: A list of n-grams (each n-gram is a tuple of words) if the text
                                                 exceeds the specified length; otherwise, a list containing the
                                                 original text.
    """
    text_words = text.split()
    if len(text_words) > length:
        ngram_list = list(ngrams(text_words, n))
    else:
        ngram_list = [text]
    return ngram_list


def split_text(text: str) -> List[str]:
    """
    Split the input text by specified delimiters while avoiding splits within numbers or weekdays.

    Eg 'Mon - Fri' should be kept as one string, while '- Salary 30k - Pension 5%' should be split into 'Salary 30k' and 'Pension 5%'.

    Args:
        text (str): The input text to be split.

    Returns:
        List[str]: A list of segments resulting from the split operation.
    """

    # List of weekdays and their abbreviations
    weekdays = [
        "Monday",
        "Mon",
        "Tuesday",
        "Tues",
        "Wednesday",
        "Wed",
        "Thursday",
        "Thu",
        "Friday",
        "Fri",
        "Saturday",
        "Sat",
        "Sunday",
        "Sun",
    ]

    # Regular expression pattern to match weekdays as whole words
    weekdays_pattern = "|".join(map(re.escape, weekdays))

    # Regular expression to split by the delimiters but not within numbers or weekdays
    split_pattern = r"(?<!\d)[,;:\(\)](?!\d)|(?<!\d) - (?!\d)"

    # Split the text using the initial pattern
    initial_splits = re.split(split_pattern, text)

    # Further split to ensure no split happens between weekdays
    final_splits = []
    buffer = ""

    for segment in initial_splits:
        if re.search(rf"\b(?:{weekdays_pattern})\b", buffer) and re.search(
            rf"\b(?:{weekdays_pattern})\b", segment
        ):
            buffer += " - " + segment
        else:
            if buffer:
                final_splits.append(buffer)
            buffer = segment

    if buffer:
        final_splits.append(buffer)

    return final_splits


def split_into_chunks(
    sentence: str, chunk_size: int = 25, overlap: int = 5
) -> List[str]:
    """
    Splits a sentence into overlapping chunks of a specified size.

    This is useful because although we've tried to split up job adverts into reasonable sentences by various means
    eg converting bullet points to full stops, splitting on certain delimiters, etc, there are still some very long sentences.
    Having an overlap of 5 words means that if there is a phrase that you'd want to capture whole eg "learning and development",
    we should still get that.

    A chunk size of 25 is recommended because EDA of a random sample of 10,000 job ads showed that the upper quartile for
    number of words in a sentence was 23 (we round up to 25).

    Args:
        sentence (str): The sentence to be split into chunks.
        chunk_size (int): The size of each chunk. Default is 25.
        overlap (int): The number of overlapping words between chunks. Default is 5.

    Returns:
        List[str]: A list of sentence chunks.
    """
    words = sentence.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk = words[i : i + chunk_size]
        chunks.append(" ".join(chunk))
        i += chunk_size - overlap
    return chunks


class JobQuality(object):
    """Find aspects of job quality from job adverts
    Args:
        JQ_THRESHOLD (float, optional): Threshold to use for predicting 1 from the logistic regression sentence quality classifier.
        CS_THRESHOLD (float, optional): The cosine similarity threshold for determining a match.
        batch_size (int, optional):
        MAX_LENGTH (int, optional): Maximum token length for fine-tuned jobbert model.
    """

    def __init__(
        self,
        JQ_THRESHOLD: float = jobbert_config["jq_threshold"],
        CS_THRESHOLD: float = jobbert_config["cs_threshold"],
        batch_size: int = jobbert_config["train_config"]["per_device_train_batch_size"],
        MAX_LENGTH: int = jobbert_config["max_length"],
    ):
        self.JQ_THRESHOLD = JQ_THRESHOLD
        self.CS_THRESHOLD = CS_THRESHOLD
        self.batch_size = batch_size
        self.MAX_LENGTH = MAX_LENGTH

        self.device = "cuda" if torch.cuda.is_available() else "cpu"

    def load(self):
        """
        Load the neccessary models and variables
        """

        logging.info(f"Loading models and variables")

        nltk.download("punkt")
        nltk.download("stopwords")

        # The sentence embedding model to use for encoding the sentences for the sentence classifier.
        #  not used
        # self.sentence_classifier_bert_transformer = SentenceTransformer(
        #     "jjzha/jobbert-base-cased", device=self.device
        # )

        # The sentence embedding model to use for encoding the n-grams and target phrases.
        self.ngram_match_bert_transformer = SentenceTransformer(
            "all-MiniLM-L6-v2", device=self.device
        )
        # self.ngram_match_bert_transformer.max_seq_length = self.MAX_LENGTH #I don't think this part needs truncation

        sentence_classifier_model, sentence_classifier_tokenizer = get_jobbert_jq(
            max_length=self.MAX_LENGTH
        )

        self.job_quality_classifier = pipeline(
            "text-classification",
            model=sentence_classifier_model,
            tokenizer=sentence_classifier_tokenizer,
            return_all_scores=False,
            device=0 if torch.cuda.is_available() else -1,  # Use GPU if available
        )

        # DataFrame containing the target phrases to match against.
        self.LOOKUP = get_keywords()
        self.tp_to_subcategory = dict(
            zip(self.LOOKUP["target_phrase"], self.LOOKUP["subcategory"])
        )
        self.target_phrases = self.LOOKUP["target_phrase"].tolist()
        # Calculate the embeddings for the target phrases since this is only needs to happen once
        logging.info(
            f"Calculating embeddings for {len(self.target_phrases)} target phrases ..."
        )
        self.target_embeddings = self.ngram_match_bert_transformer.encode(
            self.target_phrases
        )

    def extract_job_quality_sentences(
        self,
        job_adverts: Union[pd.DataFrame, List[str], str],
        id_col: str = "id",
        text_col: str = "clean_description",
        clean_job_adverts: bool = True,
    ) -> pd.DataFrame:
        """Extracts sentences from job advertisements and predicts the job quality for each sentence.
        The output dataframe contains sentences where the probability of being about job quality is greater than the threshold.

        Args:
            job_adverts (Union[pd.DataFrame, List[str], str]): Dataframe, list of strings, or string containing job adverts.
            id_col (str, optional): Unique identifier for job adverts. Defaults to "id".
            text_col (str, optional): Column containing the advert text. Defaults to "clean_description".

        Returns:
            pd.DataFrame: _description_
        """
        if isinstance(job_adverts, str):
            jobs_df = pd.DataFrame([{id_col: 0, text_col: job_adverts}])
        elif isinstance(job_adverts, list):
            jobs_df = (
                pd.DataFrame(job_adverts, columns=[text_col])
                .reset_index()
                .rename(columns={"index": id_col})
            )
        elif isinstance(job_adverts, pd.DataFrame):
            jobs_df = job_adverts[[id_col, text_col]].copy()
        else:
            raise ValueError(
                "Input must be a DataFrame, a list of strings, or a string"
            )

        if clean_job_adverts:
            jobs_df["clean_description"] = (
                jobs_df[text_col]
                .apply(clean_text)
                .str.replace("[", "")
                .str.replace("]", "")
                .str.strip()
            )
            text_col = "clean_description"

        jobs_df["sentences"] = jobs_df[text_col].apply(lambda x: sent_tokenize(x))
        jobs_df = jobs_df.explode("sentences")

        # Make sure there are no super super long sentences
        jobs_df["chunks"] = jobs_df["sentences"].apply(
            lambda x: split_into_chunks(x) if len(x.split()) >= 25 else [x]
        )
        jobs_df = jobs_df.explode("chunks").reset_index(drop=True)
        jobs_df = jobs_df.drop(columns=["sentences"])
        jobs_df = jobs_df.rename(columns={"chunks": "sentences"})

        dataset = Dataset.from_pandas(jobs_df)

        logging.info(
            f"Predicting job quality sentences for {len(jobs_df)} sentences ..."
        )
        start_time = time.time()
        predictions = self.job_quality_classifier(
            dataset["sentences"], batch_size=self.batch_size
        )

        elapsed_time = time.time() - start_time
        print(f"Time taken: {elapsed_time:.2f} seconds")

        labels = []
        pred_scores = []
        for pred in predictions:
            labels.append(pred["label"])
            pred_scores.append(pred["score"])

        jobs_df["job_quality_label"] = labels
        jobs_df["job_quality_prob"] = pred_scores

        job_quality_df = jobs_df[
            (
                (jobs_df["job_quality_label"] == "LABEL_1")
                & (jobs_df["job_quality_prob"] >= self.JQ_THRESHOLD)
            )
        ]

        return job_quality_df.reset_index(drop=True)

    def extract_ngrams(
        self, job_quality_df: pd.DataFrame, col: str = "sentences"
    ) -> pd.DataFrame:
        """
        Does some extra cleaning on sentences in the job quality DataFrame and extracts ngrams.

        Parameters:
        job_quality_df (pd.DataFrame): DataFrame containing sentences with job quality information.
        col (str): The column name containing the sentences. Defaults to "sentences".

        Returns:
        pd.DataFrame: A DataFrame with additional columns for split sentences, cleaned sentences, and n-grams.
        """
        # perform further cleaning on the sentences
        job_quality_df["sentences_split"] = job_quality_df[col].apply(split_text)
        job_quality_df_long = job_quality_df.explode("sentences_split")

        # replace digits with 'X'
        job_quality_df_long["sentence_cleaned"] = job_quality_df_long[
            "sentences_split"
        ].str.replace(r"\d", "X", regex=True)

        # get unique ngrams and their counts
        job_quality_df_long["ngrams"] = job_quality_df_long["sentence_cleaned"].apply(
            lambda x: split_ngrams(x, 6, 4)
        )
        job_quality_df_long = job_quality_df_long.explode("ngrams")
        job_quality_df_long["ngrams"] = job_quality_df_long["ngrams"].apply(
            lambda x: " ".join(x) if isinstance(x, tuple) else x
        )

        return job_quality_df_long

    def match_to_lookup(
        self,
        ngrams: Union[pd.Series, List[str]],
    ) -> pd.DataFrame:
        """
        Matches n-grams to a lookup table of target phrases using cosine similarity.

        Parameters:
        ngrams (Union[pd.Series, List[str]]): The n-grams to be matched.

        Returns:
        pd.DataFrame: A DataFrame containing n-grams, their most similar target phrases, and cosine similarities.
        """

        logging.info(f"Calculating embeddings for {len(ngrams)} ngrams ...")
        start_time = time.time()
        ngram_embeddings = self.ngram_match_bert_transformer.encode(ngrams)
        elapsed_time = time.time() - start_time
        print(f"Time taken: {elapsed_time:.2f} seconds")

        similarities = cosine_similarity(ngram_embeddings, self.target_embeddings)

        # Find the index of the highest cosine similarity for each n-gram/lookup phrase combination
        max_indices = np.argmax(similarities, axis=1)

        # Retrieve the text of the corresponding target phrases
        most_similar_phrases = [self.target_phrases[index] for index in max_indices]

        most_similar_similarities = [
            similarities[i, index] for i, index in enumerate(max_indices)
        ]

        most_similar_pairs = list(
            zip(ngrams, most_similar_phrases, most_similar_similarities)
        )

        matches = pd.DataFrame(
            most_similar_pairs, columns=["ngrams", "target_phrase", "cosine_similarity"]
        )

        matches["subcategory"] = matches["target_phrase"].map(self.tp_to_subcategory)

        # Use a specific threshold depending on the subcategory, if there isn't one
        # use the generic threshold in the 'OTHER' category
        return matches[
            matches.apply(
                lambda x: x["cosine_similarity"]
                >= self.CS_THRESHOLD.get(x["subcategory"], self.CS_THRESHOLD["OTHER"]),
                axis=1,
            )
        ]

    def match_ngrams_to_adverts(
        self, matches: pd.DataFrame, job_quality_df_long: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merges a dataframe of matched n-grams with a dataframe of job advertisements and filters the results based on cosine similarity.

        Parameters:
        matches (pd.DataFrame): DataFrame containing n-grams and their matched target phrases with cosine similarities.
        job_quality_df_long (pd.DataFrame): DataFrame containing job quality information with n-grams.

        Returns:
        pd.DataFrame: A filtered DataFrame with matched n-grams and their corresponding job advertisements.
        """

        job_quality_df_long = pd.merge(
            job_quality_df_long,
            matches,
            how="left",
            left_on="ngrams",
            right_on="ngrams",
        )

        # Group by 'sentence_cleaned' and 'target_phrase' and find index of max cosine similarity so that you don't get multiple matches
        # to the same target phrase
        idx = job_quality_df_long.groupby(["sentence_cleaned", "target_phrase"])[
            "cosine_similarity"
        ].idxmax()

        # Select the rows with these indices
        jq_df_filtered = job_quality_df_long.loc[idx].reset_index(drop=True)

        return jq_df_filtered.drop(
            ["sentences", "job_quality_prob", "sentence_cleaned"], axis=1
        )

    def extract_job_quality(
        self,
        job_adverts: Union[pd.DataFrame, List[str], str],
        id_col: str = "id",
        text_col: str = "clean_description",
    ):
        """
        Args:
            job_adverts (Union[pd.DataFrame, List[str], str]): Dataframe, list of strings, or string containing job adverts.
            id_col (str, optional): Unique identifier for job adverts. Defaults to "id".
            text_col (str, optional): Column containing the advert text. Defaults to "clean_description".

        """

        # TO DO. If len(job_adverts)>1000 do this in a loop? (may be too much to process 100k at a time)

        self.job_quality_df = self.extract_job_quality_sentences(
            job_adverts,
            id_col=id_col,
            text_col=text_col,
        )
        self.job_quality_df_long = self.extract_ngrams(self.job_quality_df)

        ngram_counts = pd.DataFrame(
            self.job_quality_df_long["ngrams"].value_counts()
        ).reset_index()

        unique_ngrams = ngram_counts["ngrams"]

        self.matches = self.match_to_lookup(unique_ngrams)

        jq_df_filtered = self.match_ngrams_to_adverts(
            self.matches, self.job_quality_df_long
        )

        job_id_to_target_phrase = (
            jq_df_filtered.groupby("id")["target_phrase"]
            .apply(lambda x: x.tolist())
            .to_dict()
        )

        return jq_df_filtered, job_id_to_target_phrase


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to run with command line arguments."
    )

    parser.add_argument(
        "--production",
        default=False,
        type=bool,
        help="Run the script in production mode or test",
    )

    parser.add_argument(
        "--sample_size",
        default=10,
        type=int,
        help="Number of job adverts to test on (only used if production=False)",
    )

    parser.add_argument(
        "--job_ads_type",
        default="afs",
        type=str,
        help="Which dataset to predict job quality measures from, can be 'afs', 'random', or 'evaluation'",
    )

    parser.add_argument(
        "--start_from_chunk",
        default=0,
        type=int,
        help="Which chunk to start from. Useful for picking up where you left off",
    )

    args = parser.parse_args()
    logging.info(args)

    if not args.production:
        chunk_size = 20
    else:
        chunk_size = 1000

    # Import the job adverts

    if args.job_ads_type == "afs":
        job_adverts = get_stratified_sample()
        id_col = "id"
        text_col = "clean_description"
    elif args.job_ads_type == "random":
        job_adverts = get_ojo_sample()
        id_col = "id"
        text_col = "description"

    if not args.production & (len(job_adverts) > args.sample_size):
        job_adverts = job_adverts.sample(args.sample_size, random_state=42)

    logging.info(f"Sample size: {len(job_adverts)}")

    start_time = time.time()
    start_time_formatted = datetime.fromtimestamp(start_time).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"Processing started at {start_time_formatted}")

    job_quality = JobQuality()
    job_quality.load()

    job_ad_chunks = [
        job_adverts.iloc[i : i + chunk_size]
        for i in range(0, len(job_adverts), chunk_size)
    ]

    interim_folder = f"job_quality/outputs/{args.job_ads_type}/interim/production_{args.production}_n_{len(job_adverts)}"

    for i, job_chunk in tqdm(enumerate(job_ad_chunks[args.start_from_chunk :])):
        logging.info(f"Processing chunk {i+args.start_from_chunk}...")

        jq_df_filtered, _ = job_quality.extract_job_quality(
            job_chunk,
            id_col,
            text_col,
        )

        filename = f"{interim_folder}/job_ads_chunk_{i+args.start_from_chunk}.parquet"
        save_to_s3(
            BUCKET_NAME,
            jq_df_filtered[
                [
                    "id",
                    "sentences_split",
                    "ngrams",
                    "target_phrase",
                    "cosine_similarity",
                ]
            ],
            filename,
        )

    end_time = time.time()
    logging.info(
        f"Time taken to process {len(job_adverts)} texts: {end_time - start_time} seconds"
    )

    logging.info("Saving data...")
    files = get_s3_data_paths(BUCKET_NAME, interim_folder, "*.parquet")
    print(files)
    output_data = pd.DataFrame()
    for file in files:
        output_data = pd.concat([output_data, load_s3_data(BUCKET_NAME, file)])

    final_filename = f"job_quality/outputs/{args.job_ads_type}/job_ads_prod_{args.production}_n_{len(job_adverts)}.parquet"
    save_to_s3(
        BUCKET_NAME,
        output_data[
            ["id", "sentences_split", "ngrams", "target_phrase", "cosine_similarity"]
        ],
        final_filename,
    )
