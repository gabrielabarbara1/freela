import time
import os
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime
import sys
from dotenv import load_dotenv  # Importar a biblioteca dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Escopos necessários para Google Drive e Google Sheets
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets.readonly']

# Caminho do arquivo para armazenar os IDs dos arquivos já enviados
SENT_FILES_PATH = "sent_files.json"

def authenticate_service_account():
    creds_info = {
        "type": os.getenv('TYPE'),
        "project_id": os.getenv('PROJECT_ID'),
        "private_key_id": os.getenv('PRIVATE_KEY_ID'),
        "private_key": os.getenv('PRIVATE_KEY').replace('\\n', '\n'),
        "client_email": os.getenv('CLIENT_EMAIL'),
        "client_id": os.getenv('CLIENT_ID'),
        "auth_uri": os.getenv('AUTH_URI'),
        "token_uri": os.getenv('TOKEN_URI'),
        "auth_provider_x509_cert_url": os.getenv('AUTH_PROVIDER_CERT_URL'),
        "client_x509_cert_url": os.getenv('CLIENT_X509_CERT_URL')
    }
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)
    return drive_service, sheets_service  

def get_folder_email_mapping(sheets_service):
    spreadsheet_id = '1ZNMvTYi-9bic2qp-ks19BGttplDUYrVy0AOkkWtEo3o'
    range_name = 'Cópia de CNPJ!A2:B'
    
    try:
        result = sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        return {row[0]: row[1] for row in result.get('values', []) if len(row) >= 2}
    except HttpError as error:
        print(f"Erro ao acessar a planilha: {error}")
        return {}

def list_files_in_subfolder(service, folder_id):
    query = f"'{folder_id}' in parents"
    response = service.files().list(q=query, fields="files(id, name, mimeType, createdTime)").execute()
    return response.get('files', [])

def get_folder_name_by_id(service, folder_id):
    try:
        folder = service.files().get(fileId=folder_id, fields="name").execute()
        return folder.get('name', 'Pasta Desconhecida')
    except HttpError as error:
        print(f"Erro ao obter o nome da pasta: {error}")
        return 'Pasta Desconhecida'

def is_file_added_today(file):
    created_time = file['createdTime']
    created_date = datetime.datetime.strptime(created_time, '%Y-%m-%dT%H:%M:%S.%fZ').date()
    return created_date == datetime.date.today()

def send_email(subject, body, recipient_email):
    try:
        msg = MIMEMultipart()
        msg['From'] = os.getenv('EMAIL_USER')
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))
        
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
            server.send_message(msg)
        print(f"E-mail enviado com sucesso para {recipient_email}.")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")

def load_sent_files():
    """Carrega a lista de arquivos já enviados do arquivo JSON."""
    if os.path.exists(SENT_FILES_PATH):
        with open(SENT_FILES_PATH, "r") as file:
            return set(json.load(file))
    return set()

def save_sent_files(sent_file_ids):
    """Salva a lista de arquivos já enviados no arquivo JSON."""
    with open(SENT_FILES_PATH, "w") as file:
        json.dump(list(sent_file_ids), file)

def monitor_folders(drive_service, folder_email_mapping):
    sent_file_ids = load_sent_files()  # Carregar IDs já enviados

    print("Iniciando verificação de pastas...")
    
    for folder_id, email in folder_email_mapping.items():
        subfolders = list_files_in_subfolder(drive_service, folder_id)
        # Filtra apenas as subpastas que queremos processar (todas menos a última)
        subfolders_to_process = subfolders[:-1]  # Ignora a última subpasta

        for subfolder in subfolders_to_process:
            if subfolder['mimeType'] == 'application/vnd.google-apps.folder':
                # Listar arquivos na subpasta
                current_files = list_files_in_subfolder(drive_service, subfolder['id'])
                # Filtrar arquivos que foram adicionados hoje
                added_today_files = [f for f in current_files if is_file_added_today(f)]
                
                if added_today_files:
                    folder_name = get_folder_name_by_id(drive_service, folder_id)
                    folder_path = f"{folder_name} > {subfolder['name']}"
                    
                    for added_file in added_today_files:
                        if added_file['id'] not in sent_file_ids:
                            file_link = f"https://drive.google.com/file/d/{added_file['id']}/view"
                            send_email(
                                f"Arquivo adicionado a {folder_path}",
                                f"O arquivo <strong>{added_file['name']}</strong> foi adicionado a {folder_path}.<br>Link: <a href='{file_link}'>{file_link}</a>",
                                email
                            )
                            sent_file_ids.add(added_file['id'])

                    # Atualiza o arquivo JSON com os arquivos já enviados
                    save_sent_files(sent_file_ids)

            time.sleep(5)  # Pausa entre as verificações de arquivos
        time.sleep(10)  # Pausa entre as verificações de subpastas

    # Pausa de 1 min antes de encerrar
    print("Aguardando 1 min antes de encerrar...")
    time.sleep(1)  

if __name__ == '__main__':
    try:
        drive_service, sheets_service = authenticate_service_account()
        folder_email_mapping = get_folder_email_mapping(sheets_service)
        monitor_folders(drive_service, folder_email_mapping)
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        print("Script finalizado.")