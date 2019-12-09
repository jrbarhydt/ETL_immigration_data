import boto3


class Uploader:
    def __init__(self):
        self.s3 = boto3.resource('s3', aws_access_key_id=self.AWS_KEY_ID, aws_secret_access_key=self.AWS_SECRET)

    def connect(self):
        # Generate the boto3 client for interacting with S3
        return boto3.client('s3', region_name='us-east-1',
                            # Set up AWS credentials
                            aws_access_key_id=self.AWS_KEY_ID,
                            aws_secret_access_key=self.AWS_SECRET)

    def list_buckets(self):
        # List the buckets
        buckets = self.s3.list_buckets()

        # Print the buckets
        print(buckets)