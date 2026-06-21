from airflow.sdk import dag, task
import pendulum


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["learning"],
)
def xcom_demo():

    @task
    def producer():
        data = {
            "name": "Bhavana",
            "role": "Data Engineer"
        }

        print("Producing data")
        return data

    @task
    def consumer(data):
        print("Received data:")
        print(data)

    result = producer()

    consumer(result)


xcom_demo()