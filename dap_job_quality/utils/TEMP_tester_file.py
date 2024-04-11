from dap_job_quality import PROJECT_DIR
from dap_job_quality.getters.ojo_getters import get_ojo_sample
from dap_job_quality.utils.text_cleaning import clean_text

from dap_job_quality.utils.spacy_keyword_search import keyword_search_df
from dap_job_quality.utils.keyword_search_patterns import keywords


import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher, Matcher
import spacy
from spacy import displacy
from spacy.matcher import Matcher


OUTPUT_PATH = PROJECT_DIR / "outputs/data/keyword_output.csv"


if __name__ == "__main__":
    raw_df = get_ojo_sample()
    raw_df["clean_description"] = raw_df["description"].apply(clean_text)
    small_df = raw_df.head(1000)

    annotated_df = keyword_search_df(small_df, keywords)
    annotated_df.to_csv(OUTPUT_PATH)
