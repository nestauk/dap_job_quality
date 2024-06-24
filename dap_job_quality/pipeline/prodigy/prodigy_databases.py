import prodigy
from prodigy.components.db import connect

# Connect to the Prodigy database
db = connect()

# Get the list of all datasets
datasets = db.datasets

# Create a dictionary to store the count of annotated examples for each dataset
dataset_counts = {}

# Iterate through each dataset and get the count of examples
for dataset in datasets:
    examples = db.get_dataset(dataset)
    dataset_counts[dataset] = len(examples)

# Print the results
for dataset, count in dataset_counts.items():
    print(f"Dataset: {dataset}, Annotated Examples: {count}")
