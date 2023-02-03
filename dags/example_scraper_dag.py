from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task
import pendulum


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    tags=['legacy']
)
def example_scraper():
    @task
    def scrape():
        k = KubernetesPodOperator(
            name="example_scraper",
            image="example_scraper",
            cmds=["python3", "scrape.py", "--scraper_type", "example_scraper", "--url", "https://www.google.com"],
            task_id="example-scraper",
            do_xcom_push=True,
        )
    scrape()

sample_dag = example_scraper()
