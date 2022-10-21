import csv
from distutils import core
import pandas as pd
from fuzzywuzzy import process, fuzz
from distutils import core


def cal_progress(current_index, num_rows_to_process):
    if current_index == round(num_rows_to_process * 0.25):
        print("25% of " + str(num_rows_to_process) + " processed")
    elif current_index == round(num_rows_to_process * 0.50):
        print("50% of " + str(num_rows_to_process) + " processed")
    elif current_index == round(num_rows_to_process * 0.75):
        print("75% of " + str(num_rows_to_process) + " processed")


base_column_name = "well id"
comparison_column_name = "UWI"

# Load files
core_df = pd.read_csv(
    "files/trial.csv",
    low_memory=False,
    usecols=[
        base_column_name,
        "formation",
        "kmax",
        "porosity",
        "grain density",
        "bulk density",
    ],
)
comparison_df = pd.read_csv(
    "files/orogo.csv",
    low_memory=False,
    usecols=[comparison_column_name, "NAD_83_LatDD", "NAD_83_LongDD"],
)

# Create output file
output_df = pd.DataFrame(
    columns=[
        "Well Id Cell",
        "Well Id Value",
        "NAD_83_LatDD",
        "NAD_83_LongDD",
        "Formation",
        "Kmax",
        "Porosity",
        "Grain Density",
        "Bulk Density",
        "Matching Cell",
        "Matching Value",
        "Percentage",
    ]
)

num_rows_to_process = core_df.shape[0]
print(
    "Number of rows to process = " + str(num_rows_to_process * comparison_df.shape[0])
)

for i, data in core_df.iterrows():
    cell_number = "A" + str(i + 2)
    cell_value = str(data[base_column_name])
    formation_value = data["formation"]
    updated_formation_value = (formation_value[1:]).capitalize()
    kmax = data["kmax"]
    porosity = data["porosity"]
    grain_density = data["grain density"]
    bulk_density = data["bulk density"]

    cal_progress(i, num_rows_to_process)

    if len(cell_value) == 0:
        continue

    for j, comp_data in comparison_df.iterrows():
        compared_cell = "P" + str(j + 2)
        compared_value = str(comp_data[comparison_column_name])
        lat = str(comp_data["NAD_83_LatDD"])
        long = str(comp_data["NAD_83_LongDD"])

        if len(compared_value) == 0:
            continue

        percentage = fuzz.ratio(cell_value, compared_value)

        if percentage < 94:
            continue
        elif compared_value[ 0 : len(compared_value)-1] != cell_value[ 0 : len(cell_value)-1]:
            continue

        output_df.loc[len(output_df.index)] = [
            cell_number,
            cell_value,
            lat,
            long,
            updated_formation_value,
            kmax,
            porosity,
            grain_density,
            bulk_density,
            compared_cell,
            compared_value,
            percentage,
        ]


output_df.to_csv("outout.csv", encoding="utf-8", index=False)
print("Done!")
