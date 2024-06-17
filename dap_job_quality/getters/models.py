import boto3
import os
import pickle

from dap_job_quality import BUCKET_NAME

s3 = boto3.client("s3")


def sentence_classifier_pca():
    file_key = "job_quality/sentence_classifier/outputs/pca.pkl"

    local_file_name = "outputs/models/sentence_classifier/pca.pkl"

    local_dir = os.path.dirname(local_file_name)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    s3.download_file(BUCKET_NAME, file_key, local_file_name)

    # Load the model using pickle
    with open(local_file_name, "rb") as file:
        pca = pickle.load(file)

    return pca


def sentence_classifier_lr():
    file_key = "job_quality/sentence_classifier/outputs/logistic_regression.pkl"

    local_file_name = "outputs/models/sentence_classifier/logistic_regression.pkl"
    s3.download_file(BUCKET_NAME, file_key, local_file_name)

    # Load the model using pickle
    with open(local_file_name, "rb") as file:
        logistic_regression_model = pickle.load(file)

    return logistic_regression_model
