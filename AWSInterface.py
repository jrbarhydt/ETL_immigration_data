import boto3
from pathlib import Path
import pandas as pd
import logging
import datetime
import os


class AWSHandler:
    def __init__(self, aws_key_id, aws_secret, region='us-east-1'):
        self.AWS_KEY_ID = aws_key_id
        self.AWS_SECRET = aws_secret
        self.REGION = region
        self.name = self.__class__.__name__

        # initialize logfile
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        root_log_path = './log'
        log_file = '/{}_{}.log'.format(self.name, timestamp)

        if not os.path.isdir(root_log_path):
            os.mkdir(root_log_path)
        logging.basicConfig(filename=root_log_path+log_file, level=logging.DEBUG)

        # initialize services
        self.s3 = self.connect_s3_service()
        self.sns = self.connect_sns_service()
        # TODO: add bucket iterable
        self.buckets = self.read_buckets()
        self.response = None

    @staticmethod
    def _response_parser(response):
        """
        Returns http status code of web response
        :param response:
        :return:
        """
        # TODO: parse web response to catch errors/results. i.e. response['ResponseMetadata']['HTTPStatusCode'] == 204
        return response['ResponseMetadata']['HTTPStatusCode']

    @staticmethod
    def get_url(bucket_name, object_key):
        """
        Print and return aws url given bucket name and object key
        :param bucket_name: name of bucket
        :param object_key: name of object
        :return: type(str): url of aws object
        """
        url = "http://{}.s3.amazonaws.com/{}".format(bucket_name, object_key)
        print(url)
        return url

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

    # S3 bucket methods
    def create_bucket(self, bucket_name: str):
        """
        Create bucket by name
        :param bucket_name: bucket name
        :return:
        """
        self.response = self.s3.create_bucket(Bucket=bucket_name)

    def read_buckets(self):
        """
        Print bucket names and return list of bucket names.
        :return: type(list): bucket_names
        """
        self.response = self.s3.list_buckets()
        bucket_names = [bucket['Name'] for bucket in self.response['Buckets']]
        print(bucket_names)
        return bucket_names

    def update_buckets(self):
        """
        Not implemented.
        :return:
        """
        return

    def delete_bucket(self, bucket_name: str):
        """
        Delete one bucket by name.
        :param bucket_name: bucket name must match exactly
        :return:
        """
        self.response = self.s3.delete_bucket(Bucket=bucket_name)

    def delete_buckets(self, partial_name: str):
        """
        Search through bucket names and delete buckets with partial_name in the bucket's name.
        :param partial_name: name or partial name of the bucket to delete
        :return:
        """
        self.response = self.s3.list_buckets()
        [self.s3.delete_bucket(Bucket=bucket['Name'])
         for bucket in self.response['Buckets']
         if partial_name in bucket['Name']]

    # S3 object methods
    def create_object(self, bucket_name: str, filename: str, key: str, public: bool = False):
        """
        Upload file to s3 bucket, given filename and key
        :param bucket_name: name of bucket
        :param filename: full filepath of object to upload
        :param key: object key
        :param public: if true, set to public-read
        :return:
        """
        if not Path(filename).is_file():
            print("File not found. Location: {}".format(filename))
            return

        if public:
            extra_args = {'ACL': 'public-read'}
        else:
            extra_args = {}

        if filename.split('.')[-1] == 'html':
            extra_args.update({'ContentType': 'text/html'})

        self.response = self.s3.upload_file(Bucket=bucket_name,
                                            Filename=filename,
                                            Key=key,
                                            ExtraArgs=extra_args)

    def read_objects(self, bucket_name, object_name_prefix=None):
        """
        Get objects starting with given name from bucket.
        If no objects name given, gets all items from bucket.
        :param bucket_name: name of bucket
        :param object_name_prefix: object(s) to read start(s) with...
        :return: type(list): object_names
        """
        if object_name_prefix is None:
            self.response = self.s3.list_objects(Bucket=bucket_name)
        else:
            self.response = self.s3.list_objects(Bucket=bucket_name, Prefix=object_name_prefix)

        if 'Contents' in self.response:
            object_names = [content['Key'] for content in self.response['Contents']]
        else:
            object_names = []

        print(object_names)
        return object_names

    def update_objects_to_public(self, bucket_name, object_name_prefix=None):
        """
        Sets public to objects starting with given name from bucket.
        If no objects name given, sets all items from bucket to public.
        :param bucket_name: name of bucket
        :param object_name_prefix: object(s) to update start(s) with...
        :return:
        """
        if object_name_prefix is None:
            self.response = self.s3.list_objects(Bucket=bucket_name)
        else:
            self.response = self.s3.list_objects(Bucket=bucket_name, Prefix=object_name_prefix)
        if 'Contents' in self.response:
            [self.s3.put_object_acl(Bucket=bucket_name, Key=content['Key'], ACL='public-read')
             for content in self.response['Contents']]

    def delete_objects(self, bucket_name, object_name_prefix=None):
        """
        Deletes objects starting with given name from bucket. If no objects name given, deletes all items from bucket.
        :param bucket_name: name of bucket
        :param object_name_prefix: object(s) to delete start(s) with...
        :return:
        """
        if object_name_prefix is None:
            self.response = self.s3.list_objects(Bucket=bucket_name)
        else:
            self.response = self.s3.list_objects(Bucket=bucket_name, Prefix=object_name_prefix)
        if 'Contents' in self.response:
            [self.s3.delete_object(Bucket='gid-staging', Key=content['Key'])
             for content in self.response['Contents']]

    def share_object(self, bucket_name, object_name, hours: int = 3):
        """
        Will generate url to share a given object for n hours.
        :param bucket_name: name of bucket
        :param object_name: name of object to share
        :param hours: length of time to share object
        :return:
        """
        if bucket_name not in self.read_buckets():
            return
        if object_name not in self.read_objects(bucket_name=bucket_name, object_name_prefix=object_name):
            return

        self.response = self.s3.generate_presigned_url(ClientMethod='get_object',
                                                       ExpiresIn=3600*hours,
                                                       Params={'Bucket': bucket_name, 'Key': object_name})
        print(self.response)

    # SNS methods
    def read_topics(self):
        """
        Print notification topic names and return list of names.
        :return: type(list): topic_names
        """
        self.response = self.sns.list_topics()
        topic_names = [bucket['Name'] for bucket in self.response['Buckets']]
        print(topic_names)
        return topic_names

    # EXTRACT to Pandas
    def create_dataframe(self, allowed_filetypes: list = None):
        """
        After completing read_objects method, this will create dataframe from objects in self.response.
        Filtered by file extension.
        :param allowed_filetypes: list of filetype extensions, ie ['xlsx', 'csv']
        :return: pandas dataframe concatenated objects
        """
        if allowed_filetypes is None:
            allowed_filetypes = ['csv']

        dataframe_list = []

        for file in self.response['Contents']:
            obj = self.s3.get_object(Bucket=self.response['Name'], Key=file['Key'])
            if obj['Key'].split('.')[-1] in allowed_filetypes:
                try:
                    df = pd.read_csv(obj['Body'], error_bad_lines=False, warn_bad_lines=True)
                    dataframe_list.append(df)
                except pd.io.common.CParserError:
                    print("Some data rows couldn't be parsed.")

        return pd.concat(dataframe_list)

    # TRANSFORM df to html
    @staticmethod
    def create_html(dataframe, html_name: str = None, desired_columns: list = None):
        """
        Generate an html table from a dataframe, with desired columns
        :param dataframe: pandas df
        :param html_name: path to and name of html file, generated by df.name by default
        :param desired_columns: list of columns to tabulate
        :return:
        """
        # TODO: abstract to df handler, or upload html object to s3 here
        # TODO: handle duplicate file name, overwrite?
        df_name = dataframe.name

        if html_name is None:
            html_name = './{}.html'.format(df_name)

        if desired_columns is None:
            dataframe.to_html(html_name,
                              border=0)
        else:
            dataframe.to_html(html_name,
                              columns=desired_columns,
                              border=0)
