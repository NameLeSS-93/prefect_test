import subprocess
from prefect import task


PATH = '/home/nmyakishev-93/openalex-snapshot-new'


@task
def sync_s3(path: str = PATH) -> 0:
    proc = subprocess.Popen(
        f'cd {path} && aws s3 sync "s3://openalex" "openalex-snapshot" --no-sign-request --region "us-west-1"',
        stdout=subprocess.PIPE,
        bufsize=1,
        universal_newlines=True,
    )
    proc.wait()
    return proc.returncode
