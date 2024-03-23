import subprocess
from prefect import task
import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config


s3 = boto3.resource('s3', region_name='us-west-1', config=Config(signature_version=UNSIGNED))
PATH = '/home/nmyakishev-93/openalex-snapshot-new'


def download_s3_files(bucket_name, local_directory):
    bucket = s3.Bucket(bucket_name)
    
    # Make sure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
    
    # Download files
    for object_summary in bucket.objects.all():
        s3_path = object_summary.key
        local_path = os.path.join(local_directory, s3_path)
        
        # Create any directories if needed
        local_path_directory = os.path.dirname(local_path)
        if not os.path.exists(local_path_directory):
            os.makedirs(local_path_directory)
        
        # Download file
        print(f"Downloading {s3_path} to {local_path}...")
        bucket.download_file(s3_path, local_path)


@task
def sync_s3(path: str = PATH) -> 0:
    download_s3_files('openalex', path)
    # proc = subprocess.Popen(
    #     # f'aws s3 sync "s3://openalex" "{path}" --no-sign-request --region "us-west-1"',
    #     'touch /home/nmyakishev-93/openalex-snapshot-new/test',
    #     stdout=subprocess.PIPE,
    #     bufsize=1,
    #     universal_newlines=True,
    # )
    # proc.wait()
    # return proc.returncode
