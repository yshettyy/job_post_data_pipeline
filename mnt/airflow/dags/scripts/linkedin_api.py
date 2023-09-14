import requests
import pandas as pd
import unicodedata


def jobs_linkedin(search_terms, location, page):
    url = "https://linkedin-jobs-search.p.rapidapi.com/"
    response_data = []
    data = {}

    def clean_string(string):
        cleaned_string = (
            unicodedata.normalize("NFD", string)
            .encode("ascii", "ignore")
            .decode("utf-8")
        )
        return cleaned_string

    payload = {"search_terms": search_terms, "location": location, "page": page}
    headers = {
        "content-type": "application/json",
        "X-RapidAPI-Key": "YOUR_API_KEY",
        "X-RapidAPI-Host": "linkedin-jobs-search.p.rapidapi.com",
    }
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        resp = response.json()
        response_data.extend(resp)
        for key in response_data[0].keys():
            data[key] = [d[key] for d in response_data]
        desired_keys = [
            "linkedin_job_url_cleaned",
            "company_name",
            "company_url",
            "job_title",
            "job_location",
            "posted_date",
        ]
        new_dict = {key: data[key] for key in desired_keys}
        df = pd.DataFrame.from_dict(new_dict)
        df["job_location"] = df["job_location"].apply(clean_string)
        df["company_name"] = df["company_name"].apply(clean_string)
        df.drop_duplicates(inplace=True)
        df.to_json(
            f'/opt/airflow/dags/files/{payload["search_terms"].replace(" ", "_").lower()}_{payload["location"].replace(" ", "_").lower()}_job.json',
            orient="table",
        )
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}. Request failed.")


if __name__ == "__main__":
    jobs_linkedin()
