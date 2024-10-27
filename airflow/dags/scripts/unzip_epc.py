import zipfile
import os

def extract_file(local_folder, file):
    zipped_file = f"{local_folder}{file}"
    extract_to = f"{zipped_file}"[:-4]
    try:
        with zipfile.ZipFile(zipped_file, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    except Exception as e:
        print(f"Error: {e}")

def extract_file_list(local_folder, file_list):
    for file in file_list:
        extract_file(local_folder, file)
        os.remove(f"{local_folder}{file}")