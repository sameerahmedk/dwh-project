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
        dataframes["salaries"]
        .copy()
        .merge(pd.DataFrame(dataframes["postings"]), on="job_id", how="inner")
    ).drop_duplicates()

    dataframes["DimBenefits"] = pd.DataFrame(
        pd.DataFrame(dataframes["postings"]).copy().merge(on="job_id", how="left")
    ).drop_duplicates()
    dataframes["DimBenefits"]["benefit_id"] = range(
        1, len(dataframes["DimBenefits"]) + 1
    )

    dataframes["Jobs"] = pd.DataFrame(
        dataframes["postings"].copy(),
        columns=[
            "job_id",
            "company_id",
            "salary_id",
            "title",
            "views",
            "applies",
            "remote_allowed",
            "application_type",
            "formatted_experience_level",
            "original_listed_time",
            "listed_time",
            "closed_time",
            "expiry",
            # "location",
            # "posting_domain",
            # "sponsored",
            # "job_posting_url",
            # "application_url",
        ],
    ).drop_duplicates()

    dataframes["Jobs"]["company_id"] = dataframes["Jobs"]["company_id"].map(
        dataframes["DimCompanies"].set_index("company_id")["company_id"]
    )
    dataframes["Jobs"]["salary_id"] = dataframes["Jobs"]["salary_id"].map(
        dataframes["DimSalaries"].set_index("salary_id")["salary_id"]
    )
    dataframes["Jobs"]["benefit_id"] = dataframes["Jobs"]["benefit_id"].map(
        dataframes["DimBenefits"].set_index("benefit_id")["benefit_id"]
    )

    """
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

     dataframes["DimLocations"]["location_id"] = range(
        1, len(dataframes["DimLocations"]) + 1
    )
    dataframes["Jobs"]["location_id"] = dataframes["Jobs"]["location"].map(
        dataframes["DimLocations"].set_index("address")["location_id"]
    )
    dataframes["Jobs"]["job_id"] = dataframes["Jobs"]["job_id"].map(
        dataframes["DimJobs"].set_index("job_id")["job_id"]
    )
    """

    del dataframes["companies"]
    del dataframes["company_industries"]
    del dataframes["salaries"]
    # del dataframes["postings"]
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
