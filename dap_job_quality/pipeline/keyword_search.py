"""'
This script creates an initial keyword search by sentence for terms related to job quality data, and outputs a dataframe with each sentence, whether a benefit keyword was found, and whether a harm keyword was found, and any terms found.
Because we're working through the files in google sheets to start with, it reads in and outputs data locally.
So to update the data, first download the current search terms, and place in the /inputs folder.

To run this script to generate 10,000 sentences of data, run the following command from the root directory:

python dap_job_quality/pipeline/keyword_search.py

It will output a file into outputs/data, which can be manually uploaded to google sheets for further analysis."""

from dap_job_quality import PROJECT_DIR, logger
from dap_job_quality.getters.ojo_getters import get_ojo_sample
from dap_job_quality.utils.text_cleaning import clean_text, split_sentences
import pandas as pd


def get_analysis_sample(df: pd.DataFrame, no_of_sentences: int) -> dict:
    sentences_for_analysis = {}
    for i in range(0, len(df)):
        job_id = df.iloc[i]["id"]
        text_list = split_sentences(df.iloc[i]["clean_description"])
        sentences_for_analysis[job_id] = {}  # Initialize the inner dictionary
        for j in range(0, len(text_list)):
            sentences_for_analysis[job_id][j] = text_list[j]

    sentences_df = (
        pd.DataFrame.from_dict(sentences_for_analysis, orient="index")
        .stack()
        .reset_index()
    )
    sentences_df.columns = ["job_id", "sentence_id", "sentence"]
    output = sentences_df.iloc[0:no_of_sentences, :].to_dict(orient="records")
    return output


def run_keyword_search(
    df: pd.DataFrame, search_terms: dict, no_of_sentences: int = 10000
) -> pd.DataFrame:
    data_for_search = get_analysis_sample(df, no_of_sentences)

    target_phrases = list(search_terms.keys())

    for item in data_for_search:
        sentence = item["sentence"]
        target_phrases_contained = [
            phrase for phrase in target_phrases if phrase in sentence
        ]
        item["target_phrases_found"] = target_phrases_contained

    output_df = pd.DataFrame(data_for_search)
    output_df = output_df.explode(
        "target_phrases_found"
    )  # Create one row for each target phrase found

    # Add the dimension and subcategory to the dataframe
    output_df["dimension"] = (
        output_df["target_phrases_found"]
        .map(search_terms)
        .apply(lambda x: x["dimension"] if pd.notnull(x) else None)
    )
    output_df["subcategory"] = (
        output_df["target_phrases_found"]
        .map(search_terms)
        .apply(lambda x: x["subcategory"] if pd.notnull(x) else None)
    )

    todays_date = pd.to_datetime("today").date()

    output_df.to_csv(f"{PROJECT_DIR}/outputs/data/keyword_search_{todays_date}.csv")


if __name__ == "__main__":
    # Import and clean the data
    logger.info("Downloading OJO sample from S3")
    ojo_df = get_ojo_sample()

    logger.info("Download complete - running analysis")
    ojo_df["clean_description"] = ojo_df["description"].apply(clean_text)

    # Get current search terms
    search_terms = (
        pd.read_csv(f"{PROJECT_DIR}/inputs/keyword_lookup.csv")
        .set_index("target_phrase")
        .to_dict(orient="index")
    )

    run_keyword_search(ojo_df, search_terms)

    logger.info("Analysis complete - output saved to outputs/data/")
