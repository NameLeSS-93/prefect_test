from prefect import flow, task


@task
def create_file():
    with open('test.txt', 'w') as file:
        file.write('test')


@flow(log_prints=True)
def my_flow():
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
