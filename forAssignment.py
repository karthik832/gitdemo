import boto3
import time
import json
import decimal
import pandas as pd
from io import StringIO
 
# Kinesis setup
kinesis = boto3.client("kinesis", region_name="us-east-1")
shard_id = "shardId-000000000000"
pre_shard_it = kinesis.get_shard_iterator(
    StreamName='CadabraOrders',
    ShardId=shard_id,
    ShardIteratorType='LATEST'
)
shard_it = pre_shard_it["ShardIterator"]
 
#Creating Session With Boto3.
session = boto3.Session(
aws_access_key_id='AKIAXYVW5JF2YR36M4NH',
aws_secret_access_key='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'
)
s3_res = session.resource('s3')
 
while 1==1:
        out = kinesis.get_records(ShardIterator=shard_it, Limit=100)
        for record in out['Records']:
            print(record)
            data = json.loads(record['Data'])
            df2 = pd.DataFrame.from_dict(data, orient="index")
            print('dataframe',df2)
        s3 = boto3.client('s3')
        bucket = 'txndat2907' # already created on S3
        cs