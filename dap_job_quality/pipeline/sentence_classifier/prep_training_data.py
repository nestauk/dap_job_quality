import ast
import altair as alt
from datetime import datetime
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
import umap

from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logging
from dap_job_quality.getters.labelled_data import (
    get_additional_examples_underrepresented_categories,
    get_positive_sents_labelled_for_categories,
)
from dap_job_quality.getters.data_getters import save_to_s3

model = SentenceTransformer("all-MiniLM-L6-v2")

SEED = 42
TODAY = datetime.today().strftime("%Y%m%d")

FALSE_NEGATIVES = [
    "Basic entitlement is 30.0 days (pro rata for hours worked).",
    "Location  Bromley.",
    "We also offer an i. Pad if you refer a new client to us and we recruit for them.",
    "Overtime rates",
    "7 days on call",
    "Monday start of shift to handover the following Monday start of shift.",
    "You will be given a training program when you start to help you integrate into the team and help you get up to date with their technologies as well as progression routes.",
    "Work between 8.30am and 3.30pm",
    "Salary £22,000 - 26,000p a",
    "£250 bonus",
    "You can discuss your preferred working hours options at interview.",
    "A full shift would be 9-5 but there are variations of hours on offer.",
    "Recommend a friend",
    "6-9 calls per day dependant on area size.",
    "They have just moved to some fantastic brand-new offices too based in South Cerney.",
    "We are also proud to have been named a 'Top Employer' for 5 consecutive years.",
    "You can earn up to £31,500 p a including regular overtime and bonuses.",
    "Salary  £50,000 - £60,000",
    "You will receive £250 for every candidate we place in permanent employment who has been recommended by you.",
    "relocation package",
    "This gives Colleagues and their family access to 24 7 365 support for a whole range of issues including physical, mental and financial issues.",
    "Refer a Friend",
    "one of the best companies to work for",
    "has topped the UK 'Best Companies to Work For' lists for 15 years",
    "Join one of the world's best employers!",
    "Our client are looking for a Security Cleared Pharmacy Technician to join their team on a locum basis starting as soon as possible on an ongoing basis.",
    "monthly commission structure",
    "with the chance of full-time job",
    "4 Nights out a week and can sometimes have run ins on a Saturday.",
    "Hours of work- 9-5Contract- temporary.",
]


def reduce_to_2D(vectors, random_state=SEED):
    """Helper function to reduce vectors to 2-d embeddings using UMAP, for visualisation purposes"""
    reducer = umap.UMAP(n_components=2, random_state=random_state)
    embedding = reducer.fit_transform(vectors)
    return embedding


def process_additional_data():
    additional_data = get_additional_examples_underrepresented_categories()

    # We don't need this column - it contains occasional notes for just a few rows
    additional_data = additional_data.drop(columns=["Unnamed: 13"])

    # Replace "different category" in "JQ" with values from "correct_category"
    additional_data.loc[
        additional_data["JQ"] == "different category", "JQ"
    ] = additional_data["correct_category"]

    additional_data.loc[additional_data["JQ"] == "1", "JQ"] = additional_data[
        "subcategory"
    ]

    additional_data["label"] = additional_data["JQ"].apply(
        lambda x: 0 if x == "0" or x == "not sure" else 1
    )

    return additional_data


def find_category_representation(additional_data):
    additional_positives = additional_data[additional_data["label"] == 1]
    # Drop the column "subcategory"
    additional_positives = additional_positives.drop(columns=["subcategory"])

    # Rename the column "JQ" to "subcategory"
    additional_positives = additional_positives.rename(columns={"JQ": "subcategory"})

    positive_categories = pd.concat(
        [
            positive_sents[["id", "subcategory"]],
            additional_positives[["id", "subcategory"]],
        ]
    )

    logging.info(f"Total number of positive sentences: {len(positive_categories)}")

    counts = positive_categories["subcategory"].value_counts()

    proportions = positive_categories["subcategory"].value_counts(normalize=True)

    combined_df = pd.DataFrame(
        {
            "subcategory": counts.index,
            "count": counts.values,
            "proportion": proportions.values,
        }
    )

    logging.info(f"Category representation: {combined_df}")

    combined_df.to_csv(
        PROJECT_DIR / "outputs/data/category_representation_in_positive_sentence.csv",
        index=False,
    )

    return positive_categories, additional_positives


def get_negative_example_sentences(
    df, id, sentences_col="sentences", true_sent_col="sentence"
):
    subset = df[df["id"] == id].copy()

    sentences = set(subset[sentences_col].iloc[0])
    sentences_w_spans = set(subset[true_sent_col].unique())
    neg_sentences = sentences - sentences_w_spans
    return neg_sentences


def create_neg_sent_df(positive_sents, false_negatives, additional_data):
    negative_sentences = {}

    for id in positive_sents["id"].unique():
        neg_sentences = get_negative_example_sentences(positive_sents, id)
        negative_sentences[id] = list(neg_sentences)

    neg_sent_df = pd.DataFrame(
        [
            (k, sentence)
            for k, sentences in negative_sentences.items()
            for sentence in sentences
        ],
        columns=["id", "sentence"],
    )

    mask = neg_sent_df["sentence"].apply(
        lambda x: not any(sub in x for sub in false_negatives)
    )

    # Apply the mask to filter the DataFrame
    neg_sent_df_filtered = neg_sent_df[mask]

    additional_negatives = additional_data[additional_data["label"] == 0]
    additional_negatives = additional_negatives.rename(
        columns={"sentences": "sentence"}
    )[["id", "sentence"]]

    all_negatives = pd.concat([neg_sent_df_filtered, additional_negatives])

    return all_negatives


def sample_negative_sents(all_negatives, n_positives, num_clusters=50):
    embeddings = model.encode(all_negatives["sentence"].tolist())

    embeddings_2d = reduce_to_2D(embeddings)

    num_clusters = num_clusters

    kmeans = KMeans(n_clusters=num_clusters)
    clusters = kmeans.fit_predict(embeddings_2d)

    # assign the cluster names back into the dataframe
    all_negatives["cluster"] = clusters

    all_negatives["cluster"].value_counts()

    all_negatives["x"] = embeddings_2d[:, 0]
    all_negatives["y"] = embeddings_2d[:, 1]

    # Create the Altair chart
    chart = (
        alt.Chart(all_negatives)
        .mark_circle(size=60)
        .encode(x="x:Q", y="y:Q", color="cluster:N", tooltip=["cluster", "sentence"])
        .properties(width=900, height=600)
        .interactive()
    )

    chart.save(PROJECT_DIR / "outputs/figures/negative_sentences_clusters.html")

    sample_size = round(n_positives / num_clusters)

    neg_sent_df_sample = all_negatives.groupby("cluster", group_keys=False).apply(
        lambda x: x.sample(min(len(x), sample_size))
    )

    neg_sent_df_sample["label"] = 0
    return neg_sent_df_sample[["id", "sentence", "label"]]


def get_split_ids(all_data, seed=42, train_prop=0.7):
    # Group by 'id' and calculate the proportion of each label within each 'id'
    id_labels = (
        all_data.groupby("id")["label"]
        .apply(lambda x: x.value_counts(normalize=True))
        .unstack(fill_value=0)
    )

    # Add a column with the majority label for stratification
    id_labels["majority_label"] = id_labels.idxmax(axis=1)

    # Get unique ids and their majority label
    ids_with_labels = id_labels.reset_index()[["id", "majority_label"]]

    # Split the unique ids into training, validation, and test sets using stratified sampling
    train_ids, temp_ids = train_test_split(
        ids_with_labels,
        test_size=1 - train_prop,
        stratify=ids_with_labels["majority_label"],
        random_state=seed,
    )
    val_ids, test_ids = train_test_split(
        temp_ids, test_size=0.5, stratify=temp_ids["majority_label"], random_state=seed
    )

    return train_ids, val_ids, test_ids


if __name__ == "__main__":
    additional_data = process_additional_data()

    logging.info(
        f"Label distribution in additional data: {additional_data['label'].value_counts()}"
    )

    # Read in the earlier data that we labelled for category
    positive_sents = get_positive_sents_labelled_for_categories()
    # convert the 'sentences' column back to a list
    positive_sents["sentences"] = positive_sents["sentences"].apply(ast.literal_eval)

    # check that IDs aren't duplicated
    duplicated_ids = set(additional_data["id"]).intersection(set(positive_sents["id"]))
    if len(duplicated_ids) < 1:
        logging.info("IDs are not duplicated")
    else:
        raise ValueError(
            f"IDs are duplicated! Please go back and check the data. The following IDs are duplicated: {duplicated_ids}"
        )

    positive_categories, additional_positives = find_category_representation(
        additional_data
    )

    all_negatives = create_neg_sent_df(positive_sents, FALSE_NEGATIVES, additional_data)

    logging.info(
        f"Total number of non job quality sentences (negative class): {len(all_negatives)}"
    )

    neg_sent_df_sample = sample_negative_sents(
        all_negatives, n_positives=len(positive_categories)
    )

    logging.info(
        f"Number of positives: {len(positive_categories)}. \n Number of negatives now that we have sampled them: {len(neg_sent_df_sample)}"
    )

    additional_positives = additional_positives.rename(
        columns={"sentences": "sentence"}
    )

    all_positives = pd.concat(
        [
            positive_sents[["id", "sentence", "label"]],
            additional_positives[["id", "sentence", "label"]],
        ]
    )

    # Concatenate the positive and negative examples
    all_data = pd.concat(
        [
            neg_sent_df_sample[["id", "sentence", "label"]],
            all_positives[["id", "sentence", "label"]],
        ]
    ).reset_index(drop=True)

    # save
    save_to_s3(
        BUCKET_NAME,
        all_data,
        f"job_quality/sentence_classifier/inputs/labelled/train_val_test_{TODAY}.csv",
    )

    train_ids, val_ids, test_ids = get_split_ids(all_data)

    splits = {"train": train_ids, "val": val_ids, "test": test_ids}

    for split_name, ids in splits.items():
        split_data = all_data[all_data["id"].isin(ids["id"])]
        print(f"{split_name} size: {len(split_data)}")
        logging.info(split_data["label"].value_counts())

        save_to_s3(
            BUCKET_NAME,
            ids,
            f"job_quality/sentence_classifier/inputs/labelled/{split_name}_ids.parquet",
        )
        save_to_s3(
            BUCKET_NAME,
            split_data,
            f"job_quality/sentence_classifier/inputs/labelled/{split_name}_df.parquet",
        )
