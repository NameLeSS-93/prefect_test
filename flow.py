from prefect import flow, task
import os
import time
from .aws import sync_s3


@task
def create_file():
    with open('/home/nmyakishev-93/test.txt', 'w') as file:
        file.write('test')


@task
def show_cwd():
    return os.getcwd()


@task
def task_10_sec():
    time.sleep(10)


@task
def task_5_sec():
    time.sleep(5)


@flow(log_prints=True)
def my_flow():
    sync_s3()


if __name__ == "__main__":
    my_flow.from_source(
        "https://github.com/NameLeSS-93/prefect_test.git",
        entrypoint="flow.py:my_flow",
    ).deploy(
        name="create-file",
        work_pool_name="update_openalex_infra",
    )
