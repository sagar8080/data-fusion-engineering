import os
import shutil
from google.cloud import storage


def zip_subfolders(folder_path):
    for foldername in os.listdir(folder_path):
        folder_abs_path = os.path.join(folder_path, foldername)
        if os.path.isdir(folder_abs_path):
            zip_path = f"{os.getcwd()}/zipped/{foldername}"
            shutil.make_archive(zip_path, 'zip', folder_abs_path)
            storage_client = storage.Client()
            script_bucket = storage_client.bucket('df-code-bucket')
            blob = script_bucket.blob(f"scripts/ingest-{foldername}.zip")
            blob.upload_from_filename(f"{zip_path}.zip")


def main():
    zip_subfolders(f"{os.getcwd()}/ingest/")


main()