import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from decimal import Decimal
from fnmatch import fnmatch
import gzip
import json
import numpy
import os
import pandas as pd
from pandas import DataFrame
from pathlib import Path
import pickle
import srsly
from typing import List, Dict, Optional
import yaml

from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logger

s3 = boto3.resource("s3")


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, set):
            return list(obj)
        return super(CustomJsonEncoder, self).default(obj)


def load_data(file_name: str, local: bool = True) -> DataFrame:
    """Loads data from path.
    Args:
            file_name (str): Local path to data.
    Returns:
            file (pd.DataFrame): Loaded Data in pd.DataFrame
    """
    if local:
        if fnmatch(file_name, "*.csv"):
            return pd.read_csv(file_name)
        else:
            logger.error(f'{file_name} has wrong file extension! Only supports "*.csv"')


def load_json_dict(file_name: str) -> dict:
    """Loads a dict stored in a json file from path.
    Args:
            file_name (str): Local path to json.
    Returns:
            file (dict): Loaded dict
    """
    if fnmatch(file_name, "*.json"):
        with open(file_name, "r") as file:
            return json.load(file)
    else:
        logger.error(f'{file_name} has wrong file extension! Only supports "*.json"')


def load_jsonl(file_path: str) -> List[Dict]:
    """
    Loads a JSON Lines (JSONL) file into a list of dictionaries.

    Each line in the JSONL file should be a valid JSON object. Lines that are not valid JSON
    objects will be skipped, and an error message will be printed.

    Args:
        file_path (str): The path to the JSONL file.

    Returns:
        List[Dict]: A list of dictionaries, where each dictionary represents a JSON object
                    from a line in the JSONL file.

    Raises:
        ValueError: If a line in the file is not a valid JSON object.
    """
    data = []
    with open(file_path, "r", encoding="utf-8-sig") as file:
        for line_number, line in enumerate(file, 1):
            line = line.strip()  # Remove leading/trailing whitespace
            if not line:
                # Skip empty lines
                continue
            try:
                data.append(srsly.json_loads(line))
            except ValueError as e:  # srsly raises ValueError for JSON errors
                print(f"Error parsing JSON on line {line_number}: {e}")
                # Optionally, continue to next line or handle error differently
    return data


def save_json_dict(dictionary: dict, file_name: str):
    """Saves a dict to a json file.

    Args:
            dictionary (dict): The dictionary to be saved
            file_name (str): Local path to json.
    """
    if fnmatch(file_name, "*.json"):
        with open(file_name, "w") as file:
            json.dump(dictionary, file)
    else:
        logger.error(f'{file_name} has wrong file extension! Only supports "*.json"')


def load_txt_lines(file_name: str) -> list:
    txt_list = []
    if fnmatch(file_name, "*.txt"):
        with open(file_name) as file:
            for line in file:
                txt_list.append(line.rstrip())
    else:
        logger.error(f'{file_name} has wrong file extension! Only supports "*.txt"')

    return txt_list


def save_to_s3(bucket_name: str, output_var, output_file_dir: str):
    """Saves a file to S3.

    Args:
        bucket_name (str): Bucket name.
        output_var (_type_): Output variable to save.
        output_file_dir (str): Path to save the file to.
    """

    obj = s3.Object(bucket_name, output_file_dir)

    if fnmatch(output_file_dir, "*.csv"):
        output_var.to_csv("s3://" + bucket_name + "/" + output_file_dir, index=False)
    elif fnmatch(output_file_dir, "*.parquet"):
        output_var.to_parquet(
            "s3://" + bucket_name + "/" + output_file_dir, index=False
        )
    elif fnmatch(output_file_dir, "*.pkl") or fnmatch(output_file_dir, "*.pickle"):
        obj.put(Body=pickle.dumps(output_var))
    elif fnmatch(output_file_dir, "*.gz"):
        obj.put(Body=gzip.compress(json.dumps(output_var).encode()))
    elif fnmatch(output_file_dir, "*.txt"):
        obj.put(Body=output_var)
    else:
        obj.put(Body=json.dumps(output_var, cls=CustomJsonEncoder))

    logger.info(f"Saved to s3://{bucket_name} + {output_file_dir} ...")


def load_s3_json(bucket_name, file_name):
    """
    Loads a file from S3 without replying on the file_name extension.
    Good for files which have no extension.

    Args:
        bucket_name (_type_): Bucket name.
        file_name (_type_): File name.

    Returns:
        Loaded data.
    """

    obj = s3.Object(bucket_name, file_name)
    file = obj.get()["Body"].read().decode()
    return json.loads(file)


def load_s3_jsonl(
    bucket_name: str, s3_file_name: str, local_file: str
) -> Optional[List[Dict]]:
    """Downloads a jsonl from s3 and loads it into a list of dictionaries.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_file_name (str): The S3 key for the file to be downloaded.
        local_file (str): The local file path where the downloaded file will be saved.

    Returns:
        Optional[List[Dict]]: A list of dictionaries loaded from the JSONL file.
                              Returns None if the file could not be downloaded or parsed.

    Raises:
        FileNotFoundError: Raised if the specified file in the S3 bucket is not found OR the local path can't be found.
        NoCredentialsError: Raised if AWS credentials are not available.
    """
    # Create an S3 client
    s3 = boto3.client("s3")

    output_file = None

    # Make sure that the directory where you want to store the file exists
    Path(local_file).parent.mkdir(parents=True, exist_ok=True)

    try:
        # Download the file
        s3.download_file(bucket_name, s3_file_name, local_file)
        logger.info(
            f"File {s3_file_name} downloaded from {bucket_name} to {local_file}"
        )

        output_file = load_jsonl(local_file)
    except FileNotFoundError:
        print(f"The file {s3_file_name} was not found in {bucket_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"The file {s3_file_name} was not found in bucket {bucket_name}.")
        else:
            print(f"An error occurred: {e}")
    except NoCredentialsError:
        print("Credentials not available")

    return output_file


def load_s3_data(bucket_name: str, file_name: str):
    """Load a file from an S3 location.

    Args:
        bucket_name (str): Name of the S3 bucket.
        file_name (str): Path to the file in the S3 bucket.

    Returns:
        Loaded data.
    """
    obj = s3.Object(bucket_name, file_name)
    if fnmatch(file_name, "*.jsonl.gz"):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as file:
            return [json.loads(line) for line in file]
    if fnmatch(file_name, "*.yml") or fnmatch(file_name, "*.yaml"):
        file = obj.get()["Body"].read().decode()
        return yaml.safe_load(file)
    elif fnmatch(file_name, "*.jsonl"):
        file = obj.get()["Body"].read().decode()
        return [json.loads(line) for line in file]
    elif fnmatch(file_name, "*.json.gz"):
        with gzip.GzipFile(fileobj=obj.get()["Body"]) as file:
            return json.load(file)
    elif fnmatch(file_name, "*.json"):
        file = obj.get()["Body"].read().decode()
        return json.loads(file)
    elif fnmatch(file_name, "*.csv"):
        return pd.read_csv("s3://" + bucket_name + "/" + file_name)
    elif fnmatch(file_name, "*.parquet"):
        return pd.read_parquet("s3://" + bucket_name + "/" + file_name)
    elif fnmatch(file_name, "*.pkl") or fnmatch(file_name, "*.pickle"):
        file = obj.get()["Body"].read().decode()
        return pickle.loads(file)
    else:
        logger.error(
            'Function not supported for file type other than "*.csv", "*.parquet", "*.jsonl.gz", "*.jsonl", or "*.json"'
        )


def get_s3_data_paths(bucket_name: str, root: str, file_types=["*.jsonl"]):
    """
    Get all paths to particular file types in a S3 root location

    bucket_name: The S3 bucket name
    root: The root folder to look for files in
    file_types: List of file types to look for, or one
    """
    if isinstance(file_types, str):
        file_types = [file_types]

    bucket = s3.Bucket(bucket_name)

    s3_keys = []
    for files in bucket.objects.filter(Prefix=root):
        key = files.key
        if any([fnmatch(key, pattern) for pattern in file_types]):
            s3_keys.append(key)

    return s3_keys


def load_s3_excel(bucket_name: str, file_name: str, sheet_name: str = "All"):
    """
    Getter for reading in ONS ASHE data (or other excel files as needed)

    Args:
        bucket_name (str): S3 bucket
        file_name (str): Path to the file in the S3 bucket
        sheet_name (str, optional):Name of the tab. Defaults to "All", as this is the relevant tab in all ONS ASHE files (as of Feb 2024).

    Returns:
        Loaded data (df)
    """
    return pd.read_excel("s3://" + BUCKET_NAME + "/" + file_name, sheet_name="All")
