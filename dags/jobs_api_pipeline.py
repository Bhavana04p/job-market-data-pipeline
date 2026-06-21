from airflow.sdk import dag, task
import pendulum
import requests
import psycopg2


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["api", "postgres"],
)
def jobs_api_pipeline():

    @task
    def extract_jobs():

        url = "https://remoteok.com/api"

        response = requests.get(
            url,
            headers={"User-Agent": "Airflow"}
        )

        jobs = response.json()

        return jobs[:10]

    @task
    def transform_jobs(jobs):

        transformed = []

        for job in jobs:

            if isinstance(job, dict):

                title = job.get("position")
                company = job.get("company")

                if title and company:

                    transformed.append(
                        {
                            "title": title,
                            "company": company,
                        }
                    )

        return transformed

    @task
    def load_jobs(data):

        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
        )

        cur = conn.cursor()

        for row in data:

            cur.execute(
                """
                INSERT INTO jobs(title, company)
                VALUES (%s, %s)
                """,
                (
                    row["title"],
                    row["company"],
                ),
            )

        conn.commit()

        cur.close()
        conn.close()

        print(f"Inserted {len(data)} jobs")

    jobs = extract_jobs()

    cleaned_jobs = transform_jobs(jobs)

    load_jobs(cleaned_jobs)


jobs_api_pipeline()