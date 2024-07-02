"""
The labelling exercise done [here](https://docs.google.com/spreadsheets/d/1Ol-FqmIkyCG-gBuT4NIKRBMfImf5NNzrRvoGzmw5fYw/edit?gid=2007053082#gid=2007053082)
was uploaded to s3. This script analyses the results.

See the metrics reported in [this deck](https://docs.google.com/presentation/d/1zPXDAyvUg55waYRdwDy_nei37lDcf0Yi1dA-wdZXp6k/edit#slide=id.g2e7116a2c4e_1_7)
"""
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
)

from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.data_getters import load_s3_data


def get_metrics(df, pred, true):
    y_true = df[true]
    y_pred = df[pred]

    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)

    print(f"Accuracy: {accuracy}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1 Score: {f1}")

    print(classification_report(y_true, y_pred))


if __name__ == "__main__":
    results = load_s3_data(
        BUCKET_NAME,
        "job_quality/early_years/evaluation_sample/evaluation_sentences_rated_20240621.csv",
    )

    get_metrics(results, "FLEX_HOURS_pred", "FLEX_HOURS_true")

    get_metrics(results, "FLEX_LOC_pred", "FLEX_LOC_true")

    # One was rated -1 because it was unclear if it should be 0 or 1, so we need to
    # get rid of this
    get_metrics(results[results["HOURS_true"] >= 0], "HOURS_pred", "HOURS_true")
