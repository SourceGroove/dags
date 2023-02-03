from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.decorators import dag, task
import pendulum


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    tags=['legacy']
)
def wiki_scraper():
    @task
    def scrape():
        k = KubernetesPodOperator(
            name="wiki_scraper",
            image="wiki_scraper",
            cmds=["python3", "scrape.py", "--scraper_type", "wiki_scraper", "--url", "https://en.wikipedia.org/wiki/List_of_programming_languages"],
            task_id="wiki-scraper",
            do_xcom_push=True,
        )
    scrape()

sample_dag = wiki_scraper()
