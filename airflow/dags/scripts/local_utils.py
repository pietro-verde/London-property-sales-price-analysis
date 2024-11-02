import os

def clean_folder(folder):
    try:
        files = os.listdir(folder)
        for f in files:
            os.remove(f'{folder}/{f}')
        print(f'Folder {folder} cleaned.')
    except FileNotFoundError as e:
        print(e)

def clean_folder_list(folder_list):
    for folder in folder_list:
        clean_folder(folder)