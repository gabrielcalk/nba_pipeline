import boto3
import logging
from botocore.exceptions import ClientError

class CloudWatchHandler(logging.Handler):
    def __init__(self, log_group, stream_name, region_name='us-east-1'):
        logging.Handler.__init__(self)
        self.client = boto3.client('logs', region_name=region_name)
        self.log_group = log_group
        self.stream_name = stream_name
        self.sequence_token = None

        # Create log group if it doesn't exist
        try:
            self.client.create_log_group(logGroupName=self.log_group)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise

        # Create log stream if it doesn't exist
        try:
            self.client.create_log_stream(logGroupName=self.log_group, logStreamName=self.stream_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                raise

    def emit(self, record):
        log_entry = self.format(record)
        timestamp = int(record.created * 1000)
        
        log_event = {
            'timestamp': timestamp,
            'message': log_entry
        }
        
        print(log_event)
        
        kwargs = {
            'logGroupName': self.log_group,
            'logStreamName': self.stream_name,
            'logEvents': [log_event]
        }
        
        if self.sequence_token:
            kwargs['sequenceToken'] = self.sequence_token
        
        try:
            response = self.client.put_log_events(**kwargs)
            self.sequence_token = response['nextSequenceToken']
        except ClientError as e:
            print(f"Error sending logs to CloudWatch: {e}")

def get_logger(name, log_group, stream_name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = CloudWatchHandler(log_group, stream_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

