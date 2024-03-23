from prefect import flow, task
import os
import time
from prefect_dask.task_runners import DaskTaskRunner


@task
def create_file():
    with open('/home/nmyakishev-93/test.txt', 'w') as file:
        file.write('test')


@task(task_runner=DaskTaskRunner())
def show_cwd():
    return os.getcwd()


@task(task_runner=DaskTaskRunner())
def task_10_sec():
    time.sleep(10)
    
    return 1


@task(task_runner=DaskTaskRunner())
def task_5_sec(arg):
    time.sleep(5)


@flow(log_prints=True)
def my_flow():
    
    task_10_sec()
    task_5_sec(1)
    
    result = task_10_sec()
    task_5_sec(result)
    
    create_file()
    print("File created")


if __name__ == "__main__":
    my_flow.from_source(
        "https://github.com/NameLeSS-93/prefect_test.git",
        entrypoint="flow.py:my_flow",
    ).deploy(
        name="create-file",
        work_pool_name="update_openalex_infra",
    )
