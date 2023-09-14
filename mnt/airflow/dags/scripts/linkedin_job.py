import pandas as pd
import glob


def create_linkedin_jobs(**kwargs):
    json_files = glob.glob(r"./opt/airflow/dags/files/*.json")
    dataframes = []

    for file in json_files:
        df = pd.read_json(file, orient="table")
        dataframes.append(df)

    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df["job_location"] = combined_df["job_location"].str.replace(", ", "_")
    combined_df.to_csv("/opt/airflow/dags/files/job.csv")


if __name__ == "__main__":
    create_linkedin_jobs()
