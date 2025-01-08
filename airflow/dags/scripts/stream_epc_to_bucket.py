import pandas as pd
from scripts.bucket_utils import Bucket
import pickle
import os

london_local_authorities_path = "/data/london-local-authorities.csv"
london_authorities = list(
    pd.read_csv(london_local_authorities_path)\
    ['Code']
)

# epc_columns_path = "/data/epc-columns.pkl"
# with open(epc_columns_path, "rb") as f:
#     columns = pickle.load(f)
# columns_set = set(columns) 
columns = ["ADDRESS","POSTCODE","TOTAL_FLOOR_AREA"]
columns_set = set(columns)

def transform(df):
    df = df[df['LOCAL_AUTHORITY'].isin(london_authorities)]
    cols_in_file_set = set(df.columns)
    missing_cols = columns_set - cols_in_file_set
    for col in missing_cols:
        df.loc[:,col] = "None"
    # for col in columns:
    #     df = df[df[col].notnull()]
    for col in columns:
        df[col] = df[col].astype(str)
    df = df[columns]
    return df
    
    

def stream_file_to_bucket(bucket_session,
                          local_folder, 
                          local_filename,
                          destination_folder,
                          chunk_size):

    df_iter = pd.read_csv(f"{local_folder}{local_filename}", iterator=True, chunksize=chunk_size,low_memory=False)

    for i, chunk in enumerate(df_iter):
        chunk_file_name = f"{local_filename[:-4]}_{i}.parquet"
        local_file_path = f"{local_folder}{chunk_file_name}"
        chunk = transform(chunk)
        if len(chunk)>0:
            chunk.to_parquet(local_file_path)
            with open(local_file_path, 'rb') as file:
                bucket_session.put_object(local_folder, chunk_file_name, 
                                          destination_folder, chunk_file_name)
            os.remove(local_file_path)

def stream_epc_files_to_bucket(file_list, 
                               region, 
                               namespace, 
                               bucket_name,
                               epc_local_folder,
                               epc_bucket_folder,
                               chunk_size):

    bucket_session = Bucket(region, namespace, bucket_name)
    
    for file in file_list:
        file = file[:-4]
        epc_unzipped_file_folder = file+"/"
        local_folder = f"{epc_local_folder}{epc_unzipped_file_folder}"
        stream_file_to_bucket(bucket_session = bucket_session,
                              local_folder = local_folder, 
                              local_filename = 'certificates.csv',
                            #   destination_folder = f"{epc_bucket_folder}{local_folder.split('/')[1]}/",
                              destination_folder = f"{epc_bucket_folder}{epc_unzipped_file_folder}",
                              chunk_size = chunk_size)

    bucket_session.close()
