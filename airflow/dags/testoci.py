import oci

def upload_to_oracle_storage():

    output_csv = 'test.csv'
    # Set up OCI config and client
    
    # print(f"::::: CURRENT DIR:::::{os.getcwd()}")
    config = oci.config.from_file("config/oci/config", "DEFAULT")  # Update with your config and profile
    # print(config['user'])
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    # Define bucket details
    namespace = 'lrqgbz9z6zlj'
    bucket_name = 'london-property-sales-price'
    object_name = 'testdocker.csv'

    # Upload the file to the OCI bucket
    with open(output_csv, 'rb') as file:
        object_storage_client.put_object(namespace, bucket_name, object_name, file)

    print(f"File '{object_name}' uploaded successfully to bucket '{bucket_name}' in namespace '{namespace}'.")