"""
This script creates a sub-sample of the OJO data and converts it to
a .jsonl format from which it can be annotated using Prodigy.

if you just want to save the data locally, navigate to the root project directory run:

python dap_job_quality/pipeline/prodigy/prep_data_for_labelling.py -ts

if you would also like to save to s3, navigate to the root project directory and run:
python dap_job_quality/pipeline/prodigy/prep_data_for_labelling.py -ts 500 -s3 True
"""
import plac
import srsly

from dap_job_quality.getters.labelling import get_stratified_sample
from dap_job_quality.getters.data_getters import save_to_s3

from dap_job_quality.utils.text_cleaning import clean_text
from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logger

from datetime import datetime
import os

import json
import boto3


@plac.annotations(
    to_s3=("to_s3", "option", "s3", bool),
)
def make_labelled_data(to_s3: bool = False):
    """Function to create a sub-sample of the OJO data and
    convert it to .jsonl format from which it can be annotated
    using Prodigy.

    Args:
        save_to_s3 (bool, optional): whether to save labelled data to s3.
            Defaults to False.
    """

    # load ojo sample and subset of unique job descriptions
    ojo_sample = get_stratified_sample()

    # apply minimal text cleaning to job descriptions
    ojo_sample["clean_description"] = (
        ojo_sample.description.apply(clean_text)
        .str.replace("[", "")
        .str.replace("]", "")
        .str.strip()
    )

    data_to_label = ojo_sample[["id", "clean_description"]].to_dict(orient="records")

    converted_training_data_local = []
    converted_training_data_jsonl = ""
    for data in data_to_label:
        training_data_json = {
            "text": data["clean_description"],
            "meta": {"job_id": data["id"]},
        }
        converted_training_data_local.append(training_data_json)
        # create json lines for s3
        converted_training_data_jsonl += json.dumps(
            training_data_json, ensure_ascii=False
        )
        converted_training_data_jsonl += "\n"

    # save data locally
    today_date = datetime.today().strftime("%Y-%m-%d").replace("-", "")
    data_path = PROJECT_DIR / "dap_job_quality/pipeline/prodigy/labelling_data"
    logger.info(f"saving labelled data locally to {data_path} location")

    if not data_path.exists():
        os.makedirs(data_path)

    filename = f"{today_date}_ads_to_label_ts_{len(ojo_sample)}.jsonl"

    srsly.write_jsonl(
        os.path.join(
            data_path,
            filename,
        ),
        converted_training_data_local,
    )

    if to_s3:
        logger.info("saving labelled data to s3")

        s3_path = os.path.join(
            "job_quality",
            "prodigy",
            "labelling_data",
            filename,
        )

        """
        # this is NOT being saved as a jsonl file, but as a json file
        save_to_s3(BUCKET_NAME, converted_training_data_jsonl, s3_path)
        """
        s3 = boto3.client("s3")
        s3.put_object(
            Body=converted_training_data_jsonl, Bucket="open-jobs-lake", Key=s3_path
        )


if __name__ == "__main__":
    plac.call(make_labelled_data)
