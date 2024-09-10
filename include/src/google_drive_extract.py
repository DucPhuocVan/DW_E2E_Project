from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from airflow.models import Variable
import io
import pandas as pd

class GoogleDrive:
    def authenticate(self):
        service_account_info = Variable.get('service_account_info', deserialize_json=True)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=['https://www.googleapis.com/auth/drive'])
        return credentials

    def list_files_in_folder(self, folder_id):
        credentials = self.authenticate()
        service = build('drive', 'v3', credentials=credentials)

        # Query to get all files in the folder with the given ID
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        return files

    def download_file(self, file_id):
        credentials = self.authenticate()
        service = build('drive', 'v3', credentials=credentials)

        request = service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()

        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Downloading file: {int(status.progress() * 100)}% complete.")

        file_stream.seek(0)  # Rewind the stream to the beginning
        return file_stream

    def download_csv_files_from_folder(self):
        parent_folder = Variable.get('parent_folder_Id')
        files = self.list_files_in_folder(parent_folder)

        dataframes = {}
        if files:
            for file in files:
                file_id = file['id']
                file_name = file['name']
                if not file_name.lower().startswith('sales'):
                    file_stream = self.download_file(file_id)
                    df = pd.read_csv(file_stream, encoding='ISO-8859-1')
                    # Use file_name as table_name, but sanitize to remove invalid characters
                    table_name = file_name.replace('.csv', '').replace(' ', '_').lower()
                    dataframes[table_name] = df
                    print(file_name)
        else:
            print("No CSV files found in the folder.")
        return dataframes
    
    def download_csv_files_sales_from_folder(self):
        parent_folder = Variable.get('parent_folder_Id')
        files = self.list_files_in_folder(parent_folder)

        dataframes_sales = {}
        dataframes = []
        if files:
            for file in files:
                file_id = file['id']
                file_name = file['name']
                if file_name.lower().startswith('sales'):
                    file_stream = self.download_file(file_id)
                    df = pd.read_csv(file_stream, encoding='ISO-8859-1')
                    dataframes.append(df)
                    table_name = "sales"
                    dataframes_sales[table_name] = pd.concat(dataframes, ignore_index=True)
        else:
            print("No CSV files found in the folder.")

        return dataframes_sales