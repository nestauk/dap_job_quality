"""
All utils functions for creating the quarto report
"""
import boto3
from botocore.exceptions import NoCredentialsError
import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
import plotly.express as px


from dap_job_quality import PROJECT_DIR, BUCKET_NAME

HEALTHCARE_ANALYSIS_DIR = PROJECT_DIR / "dap_job_quality/analysis/healthcare_analysis"
ITL1_FILE_PATH = HEALTHCARE_ANALYSIS_DIR / "ITL1.geojson"
S3_KEY = "job_quality/health_social_care/ITL1.geojson"


def get_itl1_shapes():
    """
    Obtained from https://www.data.gov.uk/dataset/1db83d90-b734-472a-9d14-6f3ae7a0bdf0/international-territorial-level-1-january-2021-boundaries-uk-buc
    """
    # return gpd.read_file(HEALTHCARE_ANALYSIS_DIR / "ITL1.geojson")
    if ITL1_FILE_PATH.exists():
        return gpd.read_file(ITL1_FILE_PATH)

    # Ensure the directory exists
    HEALTHCARE_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    # Download file from S3
    s3 = boto3.client("s3")
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(ITL1_FILE_PATH))
        print(f"Downloaded {S3_KEY} from S3 to {ITL1_FILE_PATH}")
    except NoCredentialsError:
        raise RuntimeError("AWS credentials not found. Ensure you have access to S3.")

    return gpd.read_file(ITL1_FILE_PATH)


def create_uk_heatmap(df, count_col="count"):
    gdf = get_itl1_shapes()

    gdf = gdf.merge(df, left_on="ITL121CD", right_on="itl_1_code", how="left")

    # Plot
    fig, ax = plt.subplots(1, 1, figsize=(10, 12))
    gdf.plot(
        column=count_col,
        cmap="viridis",
        linewidth=0.8,
        edgecolor="black",
        legend=True,
        ax=ax,
    )

    for _, row in gdf.iterrows():
        if row["geometry"].centroid:  # Check if centroid exists
            x, y = row["geometry"].centroid.x, row["geometry"].centroid.y
            ax.text(
                x,
                y,
                f"{int(row['count'])}",
                color="white",
                fontsize=10,
                ha="center",
                va="center",
                fontweight="bold",
                bbox=dict(
                    facecolor="black",
                    alpha=0.5,
                    edgecolor="none",
                    boxstyle="round,pad=0.3",
                ),
            )

    ax.set_title("Heatmap of ITL1 Regions", fontsize=14)
    plt.show()


def plot_years(year_counts):
    fig = px.bar(
        year_counts,
        x="year",
        y="count",
        text="count",
        title="Number of Healthcare-related Job Adverts per Year",
        labels={"year": "Year", "count": "Count"},
    )

    # Display count labels on bars
    fig.update_traces(textposition="outside")

    fig.update_layout(
        xaxis=dict(
            type="category",  # Treats years as discrete categories (ensures no decimals)
            tickmode="array",  # Sets specific ticks
            tickvals=year_counts["year"],  # Only show actual years
            tickformat="d",  # Ensures numbers are displayed as whole numbers
        )
    )
    return fig


def stacked_bar(counts_df, x, y, colour, sorted_order=None):
    # Create a stacked bar chart
    fig = px.bar(
        counts_df,
        x=x,
        y=y,
        color=colour,
        orientation="h",  # Horizontal bars
        # title='Proportion of Job Adverts Offering Flexible Location and Hours by SOC',
        labels={
            "Proportion": "Proportion of Adverts",
            "soc_4_digit_name": "SOC 4-Digit Name",
        },
        barmode="stack",  # Stacked bars
    )

    fig.update_layout(
        width=1000,  # Adjust width
        height=800,  # Adjust height
    )

    if sorted_order is not None:
        fig.update_yaxes(categoryorder="array", categoryarray=sorted_order)

    return fig


def grouped_bar(counts_df, x, y, colour):
    # Create a grouped bar chart (side-by-side bars)
    fig = px.bar(
        counts_df,
        x=x,
        y=y,
        color=colour,
        orientation="h",  # Horizontal bars
        # title='Proportion of Job Adverts Offering Flexible Location and Hours by SOC',
        labels={
            "Proportion": "Proportion of Adverts",
            "soc_4_digit_name": "SOC 4-Digit Name",
        },
        barmode="group",  # Grouped bars (side by side)
    )

    fig.update_layout(
        width=1000,  # Adjust width
        height=800,  # Adjust height
        yaxis={"categoryorder": "total ascending"},  # Sort bars by total proportion
    )

    return fig


def get_groups_w_valid_sample_size(
    df, grouping_cols=["soc_4_digit_name", "year"], min_sample_size=100
):
    """Find groupings that have size >= a minimum.

    Example usage:
    ```
    valid_soc_names = get_groups_w_valid_sample_size(sample_w_salaries_dimensions, ['soc_4_digit_name', 'year'], 100)
    valid_soc_names

    ['Clinical psychologists',
    'Generalist medical practitioners',
    'Health services and public health managers and directors',
    'Medical and dental technicians',
    ...
    ]
    ```
    """

    if isinstance(grouping_cols, str):
        grouping_cols = [grouping_cols]

    grouped_counts = df.groupby(grouping_cols).size()

    # If grouping by more than one column, unstack to create a wide format
    if len(grouping_cols) > 1:
        grouped_counts = grouped_counts.unstack()

        valid_groups = grouped_counts[
            (grouped_counts >= min_sample_size).all(axis=1)
        ].index.tolist()
    else:
        valid_groups = grouped_counts[grouped_counts >= min_sample_size].index.tolist()

    return valid_groups
