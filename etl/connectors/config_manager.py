import boto3
from botocore.exceptions import ClientError

def get_parameter(name):
    ssm = boto3.client('ssm', region_name='us-east-1')
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except ClientError as e:
        print(f"Error retrieving parameter {name}: {e}")
        return None
