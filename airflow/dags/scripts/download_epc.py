import requests
import os

token = os.environ['EPC_TOKEN']

def download_epc_file(local_folder, file, chunk_size):

    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    output_file = f"{local_folder}/{file}"
    url = f"https://epc.opendatacommunities.org/api/v1/files/{file}"
    headers = {
        "Authorization": f"Basic {token}"
    }
    
    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
    
        with open(output_file, "wb") as out_file:
            for chunk in response.iter_content(chunk_size):
                out_file.write(chunk)
        print(f"{file} downloaded.")
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")


def download_epc_file_list(local_folder, file_list, chunk_size):
    for file in file_list:
        download_epc_file(local_folder, file, chunk_size)