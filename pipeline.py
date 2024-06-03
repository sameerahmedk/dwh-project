import os
import sqlite3
import time

import pandas as pd
import schedule


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
    # Creating factJobPostings table
    fact_job_postings = (
        pd.DataFrame(
            dataframes["postings"].copy(),
            columns=[
                "job_id",
                "company_id",
                "views",
                "applies",
                "remote_allowed",
                "max_salary",
                "min_salary",
                "med_salary",
                "pay_period",
                "currency",
                "compensation_type",
                "company_name",
                "listed_time",
                "application_type",
                "formatted_work_type",
                "sponsored",
                # "title",
                # "formatted_experience_level",
                # "original_listed_time",
                # "closed_time",
                # "expiry",
                # "location",
                # "posting_domain",
                # "sponsored",
                # "job_posting_url",
                # "application_url",
            ],
        )
        .merge(
            pd.DataFrame(dataframes["company_industries"]).copy(),
            on="company_id",
            how="left",
            validate="many_to_many",
        )
        .merge(
            pd.DataFrame(dataframes["companies"])[
                ["company_id", "company_size"]
            ].copy(),
            on="company_id",
            how="left",
            validate="many_to_many",
        )
    )

    # Creating dimCompany table
    dim_company = (
        pd.DataFrame(
            fact_job_postings.copy(),
            columns=["company_id", "company_name", "company_size", "industry"],
        )
        .dropna(subset=["company_id"])
        .drop_duplicates(ignore_index=True)
    )
    fact_job_postings = fact_job_postings.drop(
        columns=["company_name", "company_size", "industry"]
    )

    # Creating dimSalary table
    fact_job_postings["salary_id"] = range(1, len(fact_job_postings) + 1)
    dim_salary = (
        pd.DataFrame(
            fact_job_postings.copy(),
            columns=[
                "salary_id",
                "min_salary",
                "med_salary",
                "max_salary",
                "pay_period",
                "currency",
                "compensation_type",
            ],
        )
        .dropna(subset=["salary_id"])
        .drop_duplicates(ignore_index=True)
    )

    # add salary variation that calculates variation of salary
    fact_job_postings["salary_variation"] = (
        fact_job_postings["max_salary"] - fact_job_postings["min_salary"]
    )

    # add apply_rate that calculates the rate of application to views
    fact_job_postings["apply_rate"] = (
        fact_job_postings["applies"] / fact_job_postings["views"] * 100
    )

    fact_job_postings = fact_job_postings.drop(
        columns=[
            "min_salary",
            "med_salary",
            "max_salary",
            "pay_period",
            "currency",
            "compensation_type",
        ]
    )

    # Creating dimDate table
    fact_job_postings["date_id"] = range(1, len(fact_job_postings) + 1)
    dim_date = (
        pd.DataFrame(
            fact_job_postings.copy(),
            columns=["date_id", "listed_time"],
        )
        .dropna(subset=["date_id"])
        .drop_duplicates(ignore_index=True)
    )
    fact_job_postings = fact_job_postings.drop(columns=["listed_time"])
    dim_date["listed_time"] = (
        dim_date["listed_time"].astype(str).str[:-5].astype(int)
    )  # removed decimal part and leading 0s to get correct timestamp
    dim_date["listed_time"] = pd.to_datetime(
        dim_date["listed_time"].astype(int), unit="s"
    )  # converted to datetime (timestamp originally in seconds)
    dim_date["Time"] = dim_date["listed_time"].dt.time
    dim_date["Date"] = dim_date["listed_time"].dt.date
    dim_date["Month"] = dim_date["listed_time"].dt.month
    dim_date["Quarter"] = dim_date["listed_time"].dt.quarter
    dim_date["Year"] = dim_date["listed_time"].dt.year

    # Data cleaning steps
    fact_job_postings["views"] = pd.to_numeric(
        fact_job_postings["views"], errors="coerce"
    ).fillna(0)
    fact_job_postings["applies"] = pd.to_numeric(
        fact_job_postings["applies"], errors="coerce"
    ).fillna(0)
    fact_job_postings.drop_duplicates(subset=["job_id"], inplace=True)
    fact_job_postings.dropna(
        subset=["company_id", "salary_id", "date_id"], inplace=True
    )

    del dataframes
    return {
        "FactJobPostings": fact_job_postings,
        "DimCompany": dim_company,
        "DimSalaries": dim_salary,
        "DimDates": dim_date,
    }


def load(dataframes: dict):
    # load all Dim and Fact to sql database (creating tables and inserting data with id constraints)
    conn = sqlite3.connect("star_schema.db")

    # add sql constraints
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")  # enable foreign key constraints for sqlite3

    # Create log table if not exists
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS ETLLog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            table_name TEXT,
            action TEXT,
            description TEXT
        );
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS DimCompany (
            company_id INTEGER PRIMARY KEY,
            company_name TEXT,
            company_size TEXT,
            industry TEXT
        );
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS DimSalaries (
            salary_id INTEGER PRIMARY KEY,
            min_salary REAL,
            med_salary REAL,
            max_salary REAL,
            pay_period TEXT,
            currency TEXT,
            compensation_type TEXT
        );
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS DimDates (
            date_id INTEGER PRIMARY KEY,
            listed_time TEXT,
            Time TEXT,
            Date TEXT,
            Month INTEGER,
            Quarter INTEGER,
            Year INTEGER
        );
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS FactJobPostings (
            job_id INTEGER PRIMARY KEY,
            company_id INTEGER,
            views INTEGER,
            applies INTEGER,
            remote_allowed INTEGER,
            salary_id INTEGER,
            apply_rate REAL,
            salary_variation REAL,
            date_id INTEGER,
            sponsored INTEGER,
            application_type TEXT,
            formatted_work_type TEXT,
            FOREIGN KEY (company_id) REFERENCES DimCompany(company_id),
            FOREIGN KEY (salary_id) REFERENCES DimSalaries(salary_id),
            FOREIGN KEY (date_id) REFERENCES DimDates(date_id)
        );
        """
    )

    appended_rows_count = 0
    for key, df in dataframes.items():
        existing_rows = pd.read_sql(f"SELECT COUNT(*) FROM {key}", conn).iloc[0, 0]

        df.to_sql(key, conn, if_exists="append", index=False)

        new_rows_count = pd.read_sql(f"SELECT COUNT(*) FROM {key}", conn).iloc[0, 0]
        appended_rows_count = new_rows_count - existing_rows

        if appended_rows_count > 0:
            log_description = f"Appended {appended_rows_count} new rows into {key}"
            timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute(
                "INSERT INTO ETLLog (timestamp, table_name, action, description) VALUES (?,?,?,?)",
                (timestamp, key, "INSERT", log_description),
            )

    conn.commit()
    conn.close()
    return appended_rows_count


def pipeline():
    dataframes = extract()
    dataframes = transform(dataframes)
    appended_rows_count = load(dataframes)
    print(f"ETL ran successfully, {appended_rows_count} new rows added.")

    """ output_folder = "./output/"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for key, df in dataframes.items():
        if key.startswith("Dim"):
            output_file = os.path.join(output_folder, f"{key}.csv")
            df.to_csv(output_file, index=False) """


def automate_pipeline():
    schedule.every().day.at("09:00").do(pipeline)  # run the pipeline every day at 9am

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    # pipeline()  # manually run the pipeline once
    automate_pipeline()  # automate pipeline run every day at 9am
