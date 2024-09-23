"""
This script creates the region boundaries for the merged regions used in the AFS analysis.

Usage:
```
python dap_job_quality/analysis/afs_analysis/geo_mapping.py
```
"""

import geopandas as gpd
import json

from dap_job_quality import BUCKET_NAME, PROJECT_DIR, logger
from dap_job_quality.getters.data_getters import upload_file_to_s3

# Download file from here: https://geoportal.statistics.gov.uk/datasets/c4b48a6e616a48228f14920666bc43e4/explore
# Then change this path to wherever you've stored the shapefile
GEOJSON_PATH = (
    PROJECT_DIR
    / "dap_job_quality/analysis/afs_analysis/International_Territorial_Level_1_January_2021_UK_BFC_2022_4388855722006182011.geojson"
)
OUT_NAME = "UK_ITL1_combined_regions.geojson"
OUT_PATH = PROJECT_DIR / f"dap_job_quality/analysis/afs_analysis/{OUT_NAME}"

# Define how ITL1 regions should be combined
GEO_MAPPING = {
    "north": [
        "North East (England)",
        "North West (England)",
        "Yorkshire and The Humber",
    ],
    "midlands": ["West Midlands (England)", "East Midlands (England)"],
    "south": ["South East (England)", "South West (England)", "East"],
    "london": ["London"],
}

if __name__ == "__main__":
    with open(GEOJSON_PATH, "r") as file:
        geojson_data = json.load(file)

    # Convert the dictionary to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(geojson_data["features"])

    # Create a reverse mapping
    reverse_mapping = {}
    for key, regions in GEO_MAPPING.items():
        for region in regions:
            reverse_mapping[region] = key

    # Add a new column for the combined region
    gdf["combined_region"] = gdf["ITL121NM"].map(reverse_mapping)

    # Dissolve the geometries by the new combined region
    combined_gdf = gdf.dissolve(by="combined_region")

    # Save the combined GeoDataFrame as a shapefile
    combined_gdf.to_file(OUT_PATH, driver="GeoJSON")
    # Copy the file to S3
    upload_file_to_s3(OUT_PATH, BUCKET_NAME, f"job_quality/early_years/{OUT_NAME}")
    logger.info(
        f"Uploaded {OUT_NAME} to {BUCKET_NAME}/job_quality/early_years/{OUT_NAME}"
    )
