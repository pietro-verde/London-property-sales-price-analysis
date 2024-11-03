import boto3
import os


class Bucket:
    def __init__(self, region, namespace, bucket_name):
        self.region = region,
        self.namespace = namespace,
        self.bucket_name = bucket_name

        OCI_ACCESS_KEY_ID = os.environ['OCI_ACCESS_KEY_ID']
        OCI_SECRET_ACCESS_KEY = os.environ['OCI_SECRET_ACCESS_KEY']
        OCI_REGION = region
        OCI_NAMESPACE = namespace

        session = boto3.session.Session()

        self.s3_client = session.client(
            's3',
            region_name=OCI_REGION,
            endpoint_url=f'https://{OCI_NAMESPACE}.compat.objectstorage.{OCI_REGION}.oraclecloud.com',
            aws_access_key_id=OCI_ACCESS_KEY_ID,
            aws_secret_access_key=OCI_SECRET_ACCESS_KEY
        )
    
    def delete_folder_content(self, bucket_folder):
        folder_prefix = bucket_folder

        delete_objects = []

        while True:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder_prefix)
            
            if 'Contents' in response:
                delete_objects.extend([{'Key': obj['Key']} for obj in response['Contents']])
                
                # Delete in batches of 1000
                while len(delete_objects) >= 1000:
                    self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={'Objects': delete_objects[:1000]})
                    delete_objects = delete_objects[1000:]
                
                # Check if there's more data to paginate
                if response.get('IsTruncated'):
                    continuation_token = response.get('NextContinuationToken')
                    response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=folder_prefix, ContinuationToken=continuation_token)
                else:
                    break
            else:
                break

        # Delete remaining objects (less than 1000)
        if delete_objects:
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={'Objects': delete_objects})

        print(f"All objects in the folder '{folder_prefix}' have been deleted.")


    def list_objects(self):
        obj_list = []
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
        try:
            for obj in response["Contents"]:
                obj_list.append(obj['Key'])
        except KeyError as e:
            print("No content to list.")
        return obj_list
    

    def put_object(self, 
                   local_folder, 
                   local_filename, 
                   destination_folder,
                   destination_filename):
        """
        local_folder 
        local_filename 
        destination_folder
        destination_filename
        """
        with open(f"{local_folder}{local_filename}", "rb") as file:
            self.s3_client.put_object(Body=file, 
                                      Key=f"{destination_folder}{destination_filename}",
                                      Bucket=self.bucket_name)
            
        
    def close(self):
        self.s3_client.close()






def clear_buckets(region,
                  namespace, 
                  bucket_name,
                  bucket_folder_list):

    bucket = Bucket(region, namespace, bucket_name)
    
    for folder in bucket_folder_list:
        bucket.delete_folder_content(folder)
    
    bucket.close()
