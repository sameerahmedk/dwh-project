import pandas as pd
import glob
import os


def extract():
    # Use os.walk to recursively search for CSV files in all subdirectories
    folder_path = "./dataset/"
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                csv_files.append(os.path.join(root, file))

    dataframes = {}
    # Iterate over each CSV file and read it into a dataframe
    for file in csv_files:
        # Extract the filename without extension as the dataframe name
        df_name = os.path.splitext(os.path.basename(file))[0]
        df = pd.read_csv(file)
        # Add a unique id number to each dataframe
        # df[df_name + "_id"] = range(1, len(df) + 1)
        dataframes[df_name] = pd.DataFrame(df)

    return dataframes


def transform(dataframes: dict) -> dict:
    dataframes["DimCompanies"] = (
        pd.DataFrame(
            dataframes["companies"].copy(), columns=["company_id", "name"]
        ).merge(
            pd.DataFrame(dataframes["company_industries"]).copy(),
            on="company_id",
            how="left",
        )
    ).drop_duplicates()

    dataframes["DimSalaries"] = pd.DataFrame(
        dataframes["salaries"].copy(),
        columns=[
            "salary_id",
            "max_salary",
            "min_salary",
            "med_salary",
            "pay_period",
            "currency",
        ],
    ).drop_duplicates()

    dataframes["DimJobs"] = (
        pd.DataFrame(
            dataframes["postings"].copy(),
            columns=["job_id", "title", "location", "formatted_work_type"],
        )
        .merge(
            pd.DataFrame(dataframes["benefits"])[["job_id", "type"]].copy(),
            on="job_id",
            how="left",
        )
        .rename(columns={"type": "benefit", "formatted_work_type": "work_type"})
    ).drop_duplicates()

    dataframes["DimLocations"] = pd.DataFrame(
        dataframes["companies"].copy(),
        columns=["company_id", "state", "country", "city", "zip_code", "address"],
    ).drop_duplicates()

    del dataframes["companies"]
    del dataframes["company_industries"]
    del dataframes["salaries"]
    del dataframes["postings"]
    del dataframes["benefits"]

    return dataframes


def main():
    dataframes = extract()
    dataframes = transform(dataframes)
    # load(dataframes)

    output_folder = "./output/"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for key, df in dataframes.items():
        if key.startswith("Dim"):
            output_file = os.path.join(output_folder, f"{key}.csv")
            df.to_csv(output_file, index=False)

    print("done")


if __name__ == "__main__":
    main()
