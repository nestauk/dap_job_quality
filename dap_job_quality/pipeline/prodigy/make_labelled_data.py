"""
This script creates a sub-sample of the OJO data and converts it to
a .jsonl format from which it can be annotated using Prodigy.

if you just want to save the data locally, run:

python dap_job_quality/pipeline/prodigy/make_labelled_data.py -ts 1000

if you would also like to save to s3, run:
python dap_job_quality/pipeline/prodigy/make_labelled_data.py -ts 1000 -s3 True
"""
import plac
import srsly

from dap_job_quality.getters.ojo_getters import get_ojo_sample
from dap_job_quality.getters.data_getters import save_to_s3

from dap_job_quality.utils.text_cleaning import clean_text
from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logger

from datetime import datetime
import os


@plac.annotations(
    train_size=("train_size", "option", "ts", int),
    to_s3=("to_s3", "option", "s3", bool),
    random_seed=("random_seed", "option", "rs", int),
)
def make_labelled_data(
    train_size: int = 1000, to_s3: bool = False, random_seed: int = 42
):
    """Function to create a sub-sample of the OJO data and
    convert it to .jsonl format from which it can be annotated
    using Prodigy.

    Args:
        train_size (int, optional): size of labelled data.
            Defaults to 1000.
        save_to_s3 (bool, optional): whether to save labelled data to s3.
            Defaults to False.
    """

    # load ojo sample and subset of unique job descriptions
    ojo_sample = (
        get_ojo_sample()
        .drop_duplicates(subset="description")
        .sample(frac=1, random_state=random_seed)[:train_size]  # shuffle
        .reset_index(drop=True)
    )

    # apply minimal text cleaning to job descriptions
    ojo_sample["clean_description"] = (
        ojo_sample.description.apply(clean_text)
        .str.replace("[", "")
        .str.replace("]", "")
        .str.strip()
    )

    data_to_label = ojo_sample[["id", "clean_description"]].to_dict(orient="records")

    converted_training_data = []
    for data in data_to_label:
        training_data_json = {
            "text": data["clean_description"],
            "meta": {"job_id": data["id"]},
        }
        converted_training_data.append(training_data_json)

    # save data locally
    today_date = datetime.today().strftime("%Y-%m-%d").replace("-", "")
    data_path = PROJECT_DIR / "dap_job_quality/pipeline/prodigy/labelled_data"
    logger.info(
        f"saving labelled data locally of size {train_size} to {data_path} location"
    )

    if not data_path.exists():
        os.makedirs(data_path)

    srsly.write_jsonl(
        os.path.join(
            data_path,
            f"{today_date}_ads_to_label_ts_{str(train_size)}_random_seed_{str(random_seed)}.jsonl",
        ),
        converted_training_data,
    )

    if to_s3:
        logger.info("saving labelled data to s3")
        s3_path = os.path.join(
            "job_quality",
            "prodigy",
            "labelled_data",
            f"{today_date}_ads_to_label_ts_{str(train_size)}_random_seed_{str(random_seed)}.jsonl",
        )
        # this is NOT being saved as a jsonl file, but as a json file
        save_to_s3(BUCKET_NAME, converted_training_data, s3_path)


if __name__ == "__main__":
    plac.call(make_labelled_data)
