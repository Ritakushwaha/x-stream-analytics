import boto3
from boto.kinesis.exceptions import ResourceInUseException
import os
import time

if aws_profile:
    os.environ['AWS_PROFILE'] = aws_profile

# connect to the kinesis
kinesis = boto.kinesis.connect_to_region(region)

def get_status():
    r = kinesis.describe_stream(stream_name)
    description = r.get('StreamDescription')
    status = description.get('StreamStatus')
    return status

def create_stream(stream_name):
    try:
        # create the stream
        kinesis.create_stream(stream_name, 1)
        print('stream {} created in region {}'.format(stream_name, region))
    except ResourceInUseException:
        print('stream {} already exists in region {}'.format(stream_name, region))


    # wait for the stream to become active
    while get_status() != 'ACTIVE':
        time.sleep(1)
    print('stream {} is active'.format(stream_name))