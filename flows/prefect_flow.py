from prefect import flow, task
import pipeline.etl_pipeline as etl

@task
def etl_task():
    etl.run_etl()

@flow(name="KaStack Order ETL Pipeline")
def kastack_flow():
    etl_task()

if __name__ == "__main__":
    kastack_flow()
