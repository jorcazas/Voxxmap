# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 22:44:15 2021

@author: javi2
"""

from __future__ import print_function
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SERVICE_ACCOUNT_FILE = 'keys.json' #Credenciales de Google Cloud

creds = None
creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)


# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1FvXedhjqj73MKXNi78o29VcIhmwWTleeumSDnS6IUY4' #ID de spreadsheet

service = build('sheets', 'v4', credentials=creds) #para llamar a la API de Google
sheet_range="Hoja1!A1:B1" #Rango a leer

# Call the Sheets API
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
                            range = sheet_range).execute()

values = result.get('values', []) #regresa los valores en una lista de listas

print(values)



