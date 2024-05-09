import datetime
import pandas as pd
import plac

from dap_job_quality import BUCKET_NAME, logging, config
from dap_job_quality.getters import ojo_getters as ojo
from dap_job_quality.getters.data_getters import save_to_s3

SEED = config["seed"]
SAMPLE_SIZE = 1000


@plac.annotations(
    train_size=("train_size", "option", "ts", int),
    random_seed=("random_seed", "option", "rs", int),
)
def stratify_a_sample(train_size: int = SAMPLE_SIZE, random_seed: int = SEED):
    today = datetime.date.today().strftime("%Y-%m-%d").replace("-", "")

    ojo_sample = ojo.get_ojo_sample().drop_duplicates(subset="description")
    titles = ojo.get_ojo_job_title_sample()
    locations = ojo.get_ojo_location_sample()
    salaries = ojo.get_ojo_salaries_sample()

    ojo_sample = pd.merge(
        ojo_sample,
        salaries[["id", "min_annualised_salary", "max_annualised_salary"]],
        on="id",
        how="left",
    )
    ojo_sample = pd.merge(
        ojo_sample, locations[["id", "itl_1_name"]], on="id", how="left"
    )
    ojo_sample = pd.merge(
        ojo_sample,
        titles[["id", "sector", "parent_sector", "knowledge_domain", "occupation"]],
        on="id",
        how="left",
    )

    # We will use median salary rather than max on the assumption that people are more likely to get hired at the minimum of their pay band?
    # Fill in NA salary values. The salary distributions has a right tail, so we use the median and not the mean.
    logging.info(
        f"Proportion of missing values in min_annualised_salary: {ojo_sample['min_annualised_salary'].isna().sum() / len(ojo_sample)}"
    )
    median_salary = ojo_sample["min_annualised_salary"].median()
    ojo_sample["min_annualised_salary"].fillna(median_salary, inplace=True)

    # Bin salary into quartiles
    ojo_sample["salary_bins"] = pd.qcut(ojo_sample["min_annualised_salary"], 4)

    # create a new column that represents the combination of other columns of interest
    ojo_sample["strata"] = (
        ojo_sample["salary_bins"].astype(str)
        + "_"
        + ojo_sample["itl_1_name"]
        + "_"
        + ojo_sample["knowledge_domain"]
    )

    # Sample 1% from each stratum (to give us a sample of about 1000)
    sampled_df = (
        ojo_sample.groupby("strata")
        .apply(
            lambda x: x.sample(
                frac=train_size / len(ojo_sample), random_state=random_seed
            )
        )
        .reset_index(drop=True)
    )

    logging.info(f"Number of ads in sample: {len(sampled_df)}")

    save_to_s3(
        BUCKET_NAME,
        sampled_df,
        f"job_quality/prodigy/labelling_data/ojo_sample_stratified_ts_{len(sampled_df)}_seed{random_seed}_{today}.csv",
    )


if __name__ == "__main__":
    plac.call(stratify_a_sample)
