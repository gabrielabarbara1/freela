import os
import time
import re
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

def limpar_cnpj(cnpj):
    return re.sub(r'\D', '', cnpj)  # Remove caracteres não numéricos

def main():
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    print("Iniciando o programa...")

    # Carregar dados do service account do .env
    service_account_info = {
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

    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    folder_id = "1C2ID5zgB8C1xBVoTIJXISrIWnvId_uVy"

    def get_latest_spreadsheet_id(drive_service, folder_id):
        query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"
        response = drive_service.files().list(q=query, orderBy="createdTime desc", pageSize=1).execute()
        files = response.get('files', [])
        return files[0]['id'] if files else None

    new_spreadsheet_id = get_latest_spreadsheet_id(drive_service, folder_id)
    source_spreadsheet_id = new_spreadsheet_id
    target_spreadsheet_id = '1jnComwkJjwe-VcknuDYZebRFK_AY1wVO1qb9zthhmHQ'
    third_spreadsheet_id = '1ZNMvTYi-9bic2qp-ks19BGttplDUYrVy0AOkkWtEo3o'

    source_range_name = 'FIN_GFE_EXP_RELATORIO_PD!A1:AH'
    third_range_name = 'Cópia de CNPJ!C2:F'
    target_range_name = 'Página1!A2:J'

    existing_data = service.spreadsheets().values().get(spreadsheetId=source_spreadsheet_id, range=source_range_name).execute()
    existing_values = existing_data.get('values', [])

    third_data = service.spreadsheets().values().get(spreadsheetId=third_spreadsheet_id, range=third_range_name).execute()
    third_values = third_data.get('values', [])

    target_data = service.spreadsheets().values().get(spreadsheetId=target_spreadsheet_id, range=target_range_name).execute()
    target_values = target_data.get('values', [])

    cnpjs_filtrados = set()
    for row in third_values:
        if len(row) > 1:
            cnpjs_filtrados.add(limpar_cnpj(row[1]))  
        if len(row) > 3:
            cnpjs_filtrados.add(limpar_cnpj(row[3]))

    ordem_indices = {row[0]: i+2 for i, row in enumerate(target_values) if len(row) > 2}

    valores_para_adicionar = []
    data_updates = []
    ordens_atualizadas = []

    for row in existing_values:
        if len(row) <= 32:
            continue

        ordem_bancaria = row[2]
        cnpj_atual = limpar_cnpj(row[32])

        if cnpj_atual not in cnpjs_filtrados:
            continue

        valores_adicionar = [row[2], row[4], row[6], row[7], row[9], row[12], row[21], row[23], row[24], row[32]]

        if ordem_bancaria in ordem_indices:
            row_index = ordem_indices[ordem_bancaria]
            range_to_update = f'Página1!A{row_index}:J{row_index}'

            if row_index - 2 < len(target_values) and target_values[row_index - 2] != valores_adicionar:
                data_updates.append({"range": range_to_update, "values": [valores_adicionar]})
                ordens_atualizadas.append(ordem_bancaria)
        else:
            valores_para_adicionar.append(valores_adicionar)

    if data_updates:
        for i in range(0, len(data_updates), 30):
            batch = data_updates[i:i+30]
            try:
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=target_spreadsheet_id,
                    body={"valueInputOption": "RAW", "data": batch}
                ).execute()
                print(f"{len(batch)} ordens bancárias atualizadas.")
                print("Ordens atualizadas:", ordens_atualizadas)
                time.sleep(10)
            except Exception as e:
                print(f"Erro ao atualizar registros: {e}")

    if valores_para_adicionar:
        valores_finais = []

        for valores_adicionar in valores_para_adicionar:
            ordem_bancaria = valores_adicionar[0]
            if ordem_bancaria not in ordem_indices:
                valores_finais.append(valores_adicionar)

        if valores_finais:
            try:
                num_linhas = len(valores_finais)

                # Inserir múltiplas novas linhas no topo
                service.spreadsheets().batchUpdate(
                    spreadsheetId=target_spreadsheet_id,
                    body={
                        "requests": [
                            {
                                "insertDimension": {
                                    "range": {
                                        "sheetId": 0,
                                        "dimension": "ROWS",
                                        "startIndex": 1,
                                        "endIndex": 1 + num_linhas
                                    },
                                    "inheritFromBefore": False
                                }
                            }
                        ]
                    }
                ).execute()

                # Inserir os valores nas novas linhas do topo
                service.spreadsheets().values().update(
                    spreadsheetId=target_spreadsheet_id,
                    range=f"Página1!A2:J{1 + num_linhas}",
                    valueInputOption="RAW",
                    body={"values": valores_finais}
                ).execute()

                print(f"{num_linhas} novas ordens bancárias inseridas no topo da tabela.")
            except Exception as e:
                print(f"Erro ao adicionar registros: {e}")
        else:
            print("Nenhuma nova ordem bancária foi encontrada para adicionar.")

    print("Finalizado com sucesso.")

if __name__ == "__main__":
    main()