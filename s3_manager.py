#!/usr/bin/env python
import logging
import random
import uuid

import argparse

from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
)
log = logging.getLogger()


def create_bucket(name, region=None):
    region = region or 'us-east-2'
    client = boto3.client('s3', region_name=region)
    params = {
        'Bucket': name,
        'CreateBucketConfiguration': {
            'LocationConstraint': region,
        }
    }
    
    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False

def list_buckets():
    s3 = boto3.resource('s3')
    
    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count += 1
    print(f'Found {count} buckets!')
    
def get_bucket(name, create=False, region=None):
    client = boto3.resource('s3')
    bucket = client.Bucket(name=name)
    if bucket.creation_date:
     return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist!')
            return

def create_tempfile(file_name=None, content=None, size=300):
    """Create a temporary text file"""
    filename = f'{file_name or uuid.uuid4().hex}.txt'
    with open(filename, 'w') as f:
        f.write(f'{(content or "0") * size}')
    return filename

def create_bucket_object(bucket_name, file_path, key_prefix=None):
    """Create a bucket object
    :params bucket_name: The target bucket
    :params type: str
    :params file_path: The path to the file to be uploaded to the bucket
    .
    :params type: str
    :params key_prefix: Optional prefix to set in the bucket for the fil
    e.
    :params type: str
    """
    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object

def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
    """Download a bucket object
    :params bucket_name: The target bucket
    :params type: str
    :params object_key: The bucket object to get
    :params type: str
    :params dest: Optional location where the downloaded file will
    stored in your local.
    :params type: str
    :returns: The bucket object and downloaded file path object.
    :rtype: tuple
    """
    bucket = get_bucket(bucket_name)
    params = {'key': object_key}
    if version_id:
        params['VersionId'] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f'{file_path}')
    return bucket_object, file_path
    
def enable_bucket_versioning(bucket_name):
    """Enable bucket versioning for the given bucket_name
    """
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status

def delete_bucket_objects(bucket_name, key_prefix=None):
    """Delete all bucket objects including all versions
    of versioned objects.
    """
    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()
    targets = [] # This should be a max of 1000
    for obj in objects:
        targets.append({
            'Key': obj.object_key,
            'VersionId': obj.version_id,
        })
        
    bucket.delete_objects(Delete={
        'Objects': targets,
        'Quiet': True,
    })
    return len(targets)
    
def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
    else:
        count = 0
        client = boto3.resource('s3')
        for bucket in client.buckets.iterator():
            try:
                bucket.delete()
                bucket.wait_until_not_exists()
                count += 1
            except ClientError as err:
                log.warning(f'Bucket {bucket.name}: {err}')
    return count

if __name__ == '__main__':    
    parser = argparse.ArgumentParser(description='AWS S3 Bucket Operations')
    
    subparsers=parser.add_subparsers(title="Commands", dest="command")
    
    #Create bucket
    sp_create_bucket=subparsers.add_parser('create_bucket',  help='Create bucket')
    sp_create_bucket.add_argument('name', type=str, help='Bucket name')
    sp_create_bucket.add_argument('--region', type=str, default='us-east-2', help='AWS Region')
    
    #List buckets
    sp_list_bucket=subparsers.add_parser('list_buckets',  help='List buckets')
    
    #Get a bucket
    sp_get_bucket=subparsers.add_parser('get_bucket',  help='Get bucket')
    sp_get_bucket.add_argument('name', type=str, help='Bucket name')
    sp_get_bucket.add_argument('--region', type=str, default='us-east-2', help='AWS Region')
    
    #Create a bucket object
    sp_create_bucket_object=subparsers.add_parser('create_bucket_object',  help='Create bucket Object')
    sp_create_bucket_object.add_argument('bucket_name', type=str, help='The target bucket')
    sp_create_bucket_object.add_argument('file_path', type=str, help=' The path to the file to be uploaded to the bucket')
    sp_create_bucket_object.add_argument('--key_prefix', type=str, help='Optional prefix to set in the bucket for the file')
    
    #Get bucket object
    sp_get_bucket_object=subparsers.add_parser('get_bucket_object',  help='Get bucket Object')
    sp_get_bucket_object.add_argument('bucket_name', type=str, help='The target bucket')
    sp_get_bucket_object.add_argument('object_key', type=str, help='The object to get')
    sp_get_bucket_object.add_argument('--dest', type=str, help='Optional location where downloaded file will be stored')
    sp_get_bucket_object.add_argument('--version_id', type=str, help='Optional version id')


    #Enable bucket object version
    sp_enable_bucket_versioning=subparsers.add_parser('enable_bucket_versioning', help='Enables versioning of target bucket')
    sp_enable_bucket_versioning.add_argument('bucket_name', type=str, help='The target bucket')
    
    #Delete a bucket object
    sp_delete_bucket_objects=subparsers.add_parser('delete_bucket_objects', help='Delete all bucket objects including all versions of versioned objects.')
    sp_delete_bucket_objects.add_argument('bucket_name', type=str, help='The target bucket')
    sp_delete_bucket_objects.add_argument('--key_prefix', type=str, help='Optional prefix to set in the bucket for the file')
    

    #Delete buckets
    sp_delete_buckets=subparsers.add_parser('delete_buckets',  help='Delete buckets')
    sp_delete_buckets.add_argument('--name', type=str, help='Optional Bucket name to be deleted')


    args = parser.parse_args()
    
#----Conditional statements to determine which actions to take
    
if args.command == 'create_bucket':
    create_bucket(args.name, region=args.region)
elif args.command == "list_buckets":
    list_buckets()
elif args.command == "get_bucket":
    get_bucket(args.name, region=args.region)
elif args.command == "create_bucket_object":
    create_bucket_object(args.bucket_name, args.file_path, args.key_prefix)
elif args.command == "get_bucket_object":
    get_bucket_object(args.bucket_name, args.object_key, args.dest, args.version_id)
elif args.command == "enable_bucket_versioning":
    enable_bucket_versioning(args.bucket_name)
elif args.command == "delete_bucket_objects":
    delete_bucket_objects(args.bucket_name, args.key_prefix)
elif args.command == "delete_buckets":
    delete_buckets(args.name)
    

