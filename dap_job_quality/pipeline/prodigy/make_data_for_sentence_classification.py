import json
import numpy as np
import pandas as pd

from dap_job_quality import PROJECT_DIR
from dap_job_quality.getters.ojo_getters import get_ojo_sample
import dap_job_quality.utils.text_cleaning as tc

# Set the random seed
np.random.seed(42)

OUT_FILE = PROJECT_DIR / "inputs/labelling/job_sentences.jsonl"

if __name__ == "__main__":
    unlabelled_data = get_ojo_sample()
    unlabelled_data["clean_description"] = unlabelled_data["description"].apply(
        tc.clean_text
    )
    unlabelled_data["created"] = pd.to_datetime(unlabelled_data["created"])

    recent_data = unlabelled_data[unlabelled_data["created"] >= "2022-01-01"]

    unique_ids = recent_data["id"].unique()

    # Sample 500 unique IDs without replacement
    sampled_ids = np.random.choice(unique_ids, size=500, replace=False)

    # Filter the DataFrame to only include rows with the sampled IDs
    sampled_df = recent_data[recent_data["id"].isin(sampled_ids)]

    output_list = (
        sampled_df[["id", "clean_description"]]
        .rename(columns={"clean_description": "text"})
        .to_dict(orient="records")
    )

    with open(OUT_FILE, "w") as outfile:
        for item in output_list:
            json_str = json.dumps(item)
            outfile.write(json_str + "\n")
