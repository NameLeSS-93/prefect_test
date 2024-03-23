import subprocess
from prefect import task


PATH = '/home/nmyakishev-93/openalex-snapshot-new'


@task
def sync_s3(path: str = PATH) -> 0:
    proc = subprocess.Popen(
        # f'aws s3 sync "s3://openalex" "{path}" --no-sign-request --region "us-west-1"',
        'touch /home/nmyakishev-93/openalex-snapshot-new/test'
        stdout=subprocess.PIPE,
        bufsize=1,
        universal_newlines=True,
    )
    proc.wait()
    return proc.returncode
