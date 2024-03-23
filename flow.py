from prefect import flow, task
import os


@task
def create_file(path):
    with open(f'{path}/test.txt', 'w') as file:
        file.write('test')


@task
def show_cwd():
    return os.getcwd()


@flow(log_prints=True)
def my_flow():
    path = show_cwd()
    print(f"Got {path=}")

    create_file(path)
    print("File created")


if __name__ == "__main__":
    my_flow.from_source(
        "https://github.com/NameLeSS-93/prefect_test.git",
        entrypoint="flow.py:my_flow",
    ).deploy(
        name="create-file",
        work_pool_name="update_openalex_infra",
    )
