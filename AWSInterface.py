import boto3


class AWSHandler:
    def __init__(self, aws_key_id, aws_secret, region='us-east-1'):
        self.AWS_KEY_ID = aws_key_id
        self.AWS_SECRET = aws_secret
        self.REGION = region

        # initialize services
        self.s3 = self.connect_s3_service()
        self.sns = self.connect_sns_service()

    @staticmethod
    def _response_parser(response):
        """
        Returns http status code of web response
        :param response:
        :return:
        """
        # TODO: parse web response to catch errors/results. i.e. response['ResponseMetadata']['HTTPStatusCode'] == 204
        return response['ResponseMetadata']['HTTPStatusCode']

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
        """
        Print bucket names and return list of bucket names.
        :return: type(list): bucket_names
        """
        response_data = self.s3.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response_data['Buckets']]
        print(bucket_names)
        return bucket_names

    def create_bucket(self, bucket_name: str):
        """
        Create bucket by name
        :param bucket_name: bucket name
        :return:
        """
        self.s3.create_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name: str):
        """
        Delete one bucket by name.
        :param bucket_name: bucket name must match exactly
        :return:
        """
        self.s3.delete_bucket(Bucket=bucket_name)

    def delete_buckets(self, partial_name: str):
        """
        Search through bucket names and delete buckets with partial_name in the bucket's name.
        :param partial_name: name or partial name of the bucket to delete
        :return:
        """
        response_data = self.s3.list_buckets()
        [self.s3.delete_bucket(Bucket=bucket['Name'])
         for bucket in response_data['Buckets']
         if partial_name in bucket['Name']]

    def upload_to_bucket(self, bucket_name: str, filename: str, key: str):
        """
        Upload file to s3 bucket, given filename and key
        :param bucket_name: name of bucket
        :param filename: full filepath of object to upload
        :param key: object key
        :return:
        """
        self.s3.upload_file(Bucket=bucket_name,
                            Filename=filename,
                            Key=key)

    # SNS methods
    def list_topics(self):
        """
        Print notification topic names and return list of names.
        :return: type(list): topic_names
        """
        response_data = self.sns.list_topics()
        topic_names = [bucket['Name'] for bucket in response_data['Buckets']]
        print(topic_names)
        return topic_names
