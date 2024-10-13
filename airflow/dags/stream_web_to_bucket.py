import pandas as pd
import os
import oci

def stream_web_to_bucket(namespace, 
                             bucket_name,
                             local_folder,
                             bucket_folder,
                             csv_url,
                             chunk_size):

    header = ['TRANSACTION_UNIQUE_IDENTIFIER','PRICE',
            'DATE_OF_TRANSFER','POSTCODE','PROPERTY_TYPE',
            'OLD_NEW','DURATION','PAON','SAON','STREET',
            'LOCALITY','TOWN_CITY','DISTRICT','COUNTY',
            'PPD_CATEGORY_TYPE','RECORD_STATUS_MONTHLY_FILE_ONLY']

    config = oci.config.from_file("config/oci/config", "DEFAULT")
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    df_iter = pd.read_csv(csv_url, iterator=True, chunksize=chunk_size, header=None, names=header)

    for i, chunk in enumerate(df_iter):
        file_name = f'chunk_{i+1}.parquet'
        local_file_path = f"{local_folder}{file_name}"
        chunk.to_parquet(local_file_path)
        bucket_file_path = f"{bucket_folder}{file_name}"
        with open(local_file_path, 'rb') as file:
            object_storage_client.put_object(namespace, bucket_name, bucket_file_path, file)
            print('transfer')
        os.remove(local_file_path)