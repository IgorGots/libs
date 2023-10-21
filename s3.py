import boto3
import pickle
from io import BytesIO

class S3ObjectStorage:
    def __init__(self, bucket_name, access_key=None, secret_key=None, s3_client=None):
        self.bucket_name = bucket_name
        if s3_client is None:
            if access_key and secret_key:
                self.s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            else:
                self.s3_client = boto3.client('s3')
        else:
            self.s3_client = s3_client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass  # No special cleanup needed

    def save_pickle(self, key, data):
        try:
            data_bytes = pickle.dumps(data)
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=data_bytes)
            return True
        except Exception as e:
            print(f"Error saving pickle object: {e}")
            return False

    def load_pickle(self, key):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            data_bytes = response['Body'].read()
            loaded_data = pickle.loads(data_bytes)
            return loaded_data
        except Exception as e:
            print(f"Error loading pickle object: {e}")
            return None

    def list_objects(self):
        try:
            response = self.s3_client.list_objects(Bucket=self.bucket_name)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except Exception as e:
            print(f"Error listing objects in S3 bucket: {e}")
            return []

    def delete_object(self, key):
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            return True
        except Exception as e:
            print(f"Error deleting object from S3 bucket: {e}")
            return False

# Usage Example
if __name__ == "__main__":
    s3_bucket_name = "your-s3-bucket-name"

    with S3ObjectStorage(s3_bucket_name) as s3_pickle_storage:
        data_to_save = {'example': 'data'}

        # Save data to S3
        s3_pickle_storage.save_pickle('example.pickle', data_to_save)

        # Load data from S3
        loaded_data = s3_pickle_storage.load_pickle('example.pickle')
        print(loaded_data)

