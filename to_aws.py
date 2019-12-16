import boto3


class Uploader:
    # TODO: parse web response to catch errors/results. i.e. response['ResponseMetadata']['HTTPStatusCode'] == 204

    def __init__(self, aws_key_id, aws_secret, region='us-east-1'):
        self.AWS_KEY_ID = aws_key_id
        self.AWS_SECRET = aws_secret
        self.REGION = region

        # initialize services
        self.s3 = self.connect_s3_service()
        self.sns = self.connect_sns_service()

    def connect_s3_service(self):
        # initialize services for interacting with s3
        return boto3.client('s3', region_name=self.REGION,
                            # Set up AWS credentials
                            aws_access_key_id=self.AWS_KEY_ID,
                            aws_secret_access_key=self.AWS_SECRET)

    def connect_sns_service(self):
        # initialize services for notifications
        return boto3.client('sns', region_name=self.REGION,
                            # Set up AWS credentials
                            aws_access_key_id=self.AWS_KEY_ID,
                            aws_secret_access_key=self.AWS_SECRET)

    # S3 methods
    def list_buckets(self):
        # List the bucket names
        response_data = self.s3.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response_data['Buckets']]
        print(bucket_names)
        return bucket_names

    def create_bucket(self, bucket_name):
        self.s3.create_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name):
        self.s3.delete_bucket(Bucket=bucket_name)

    def list_topics(self):
        # List the publishable topics
        topics = self.sns.list_topics()
        print(topics)
