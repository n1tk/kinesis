
# coding: utf-8

# In[ ]:

#kinnesis pool data and write to local file

from boto import kinesis
import time
import json
import pickle

# AWS Connection Credentials
aws_access_key = '*****'
aws_access_secret = '*****'

# Selected Kinesis Stream from where to pull data
stream = 'test-dev'

# Aws Authentication
auth = {"aws_access_key_id": aws_access_key, "aws_secret_access_key": aws_access_secret}
conn = kinesis.connect_to_region('eu-west-1',**auth)

# Targeted file to be pushed to S3 bucket
fileName = "KinesisDataTest1.txt"
f = open("C:\\Users\\test\\Desktop\\data\\Kinesis_S3Data.json", "w+")

# Describe stream and get shards
tries = 0
while tries < 10:
    tries += 1
    time.sleep(1)
    response = conn.describe_stream(stream)
    if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
        break
else:
    raise TimeoutError('Stream is still not active, aborting...')

# Get Shard Iterator and get records from stream
shard_ids = []
stream_name = None
if response and 'StreamDescription' in response:
    stream_name = response['StreamDescription']['StreamName']
    for shard_id in response['StreamDescription']['Shards']:
        shard_id = shard_id['ShardId']
        shard_iterator = conn.get_shard_iterator(stream,
        shard_id, "LATEST")["ShardIterator"]
        tries = 0
        result = []
        while tries < 100:
            tries += 1
            response = conn.get_records(shard_iterator, limit=2)
            shard_iterator = response['NextShardIterator']
            if len(response['Records'])> 0:
                for res in response['Records']:
                    result.append(res['Data'])
                    f.write(str(result) + "\n")

