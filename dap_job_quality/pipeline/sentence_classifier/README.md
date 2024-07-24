# Sentence classifier pipeline

## Quick start

## Data preparation

The scripts in `training_data/` contain the code that we used to prepare and augment the labelled data we had for the sentence classifier. The different manual and automated steps that we took to prepare the data are as follows:

1. `01_concat_data.py`: This script concatenates different batches of labelled data.
2. As the data above was only labelled binarily (related to job quality/not related to job quality), we then manually labelled these sentences for job quality category [here](https://docs.google.com/spreadsheets/d/1bXNmO9vOLG6zdDpHl0Tdw9AeDqb43CrpkXzNGyHRhWI/edit?gid=4769099#gid=4769099).
3. `02_check_category_representation.py`: This script aggregates the data that has been labelled for categories and calculates the proportion of each category in the data.
4. We manually inspected the table produced in the step above. For those categories which made up the smallest proportions of the data, we added lists of search terms that could be used to find additional positive examples for these categories. The table the lists of search terms can be found [here](https://docs.google.com/spreadsheets/d/1DuD3f2V4Ffm-JyibpFaZ_tFlh7U5SlwJFi6rJxRaJ_w/edit?gid=206573346#gid=206573346).
5. `03_pad_out_sample.py`: This script searches the data for texts that contain the search terms. We took a random sample of 50 additional texts per category.
6. We then did more labelling(!): we went through the additional samples for the underrepresented categories and determined whether each one was indeed linked to the correct category or not. We carried this out [here](https://docs.google.com/spreadsheets/d/1s4SRbQypfUP0sY88y-NlaOJegHYv21whcBAVudS9Paw/edit?gid=1942213912#gid=1942213912).
7. `04_prep_training_data.py`: This script brings together all of the positive examples that were obtained at the previous steps. It also extracts negative examples to counterbalance the positive sentences. It does this by combining: sentences from the data that was collated in step (1) that were not labelled as being related to job qualtiy; sentences that were labelled at step (6) and determined to be not related to job quality. In order to ensure a balanced sample, the negative sentences are then clustered using all-miniLM-l6-v2 embeddings, UMAP, and Kmeans. A subsample was then taken from each of the clusters. In this way, we ensured that we had a negative sample that was similar in size to the positive sample, and that was representative of the different types of non-job-quality sentences that we might encounter in real job adverts.

In the end, the training, validation and test sets were balanced as follows:
| | Training| Validation | Test |
|----------|----------|----------|----------|
| 0 | 533 | 164 | |
| 1 | 577 | 163 | |

## Training the classifier
