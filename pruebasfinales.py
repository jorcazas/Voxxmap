from googleapiclient.discovery import build
from google.oauth2 import service_account
import psycopg2

def timestamp_format(timestamp_from_spreadsheet):
    hora = timestamp_from_spreadsheet.split(' ')[1]
    fecha = timestamp_from_spreadsheet.split(' ')[0]
    
    split_ts = fecha.split('/')
    new_format = split_ts[2]+'-'

    if(len(split_ts[1]) == 1):
        new_format = new_format+'0'+split_ts[1]+'-'
        
    else:
        new_format = new_format+split_ts[1]+'-'
    
    if(len(split_ts[0]) == 1):
        new_format = new_format+'0'+split_ts[0]
    else:
        new_format = new_format+split_ts[0]
    
    new_format = new_format+' '+hora
    return new_format


def get_upid(timestamp):   
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_select_query = "SELECT update_id FROM update_ where update_date = " + "'" + timestamp +"'"
        
        cursor.execute(postgres_select_query)
        
    
        connection.commit()
        count = cursor.rowcount
        #print(count, "Record inserted successfully into mobile table")
        
        return cursor.fetchone()[0]

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
    
def update(lista):
    moph=lista[1]
    personal_id = 1
    update_date = timestamp_format(lista[0])
    a1=tuple([update_date, personal_id, moph])
    
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_insert_query = """ INSERT INTO update_ (update_date,personal_id,MOPH_number) values (%s,CAST( %s AS NUMERIC),CAST( %s AS NUMERIC))"""
        
        record_to_insert = a1
        cursor.execute(postgres_insert_query, record_to_insert)
    
        connection.commit()
        count = cursor.rowcount
        #print(count, "Record inserted successfully into mobile table")
        
        return update_date

    except (Exception, psycopg2.Error) as error:
        return "Error"
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
    
            
def infraestructura(lista,upid):
    
    upid_str = str(int(upid))
    lista1 = lista[4:8]
    lista1.append(lista[10])
    lista1.append(upid_str)
    a1=tuple(lista1)    
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_insert_query = """ INSERT INTO infraestructura (screening_implemeted,awareness_campaigns,
        test_covid_capabilities,test_result_speed,resources_received_last_month,
        update_id) values (%s,%s,%s,CAST( %s AS NUMERIC),%s,CAST( %s AS NUMERIC))"""
        
        record_to_insert = a1
        cursor.execute(postgres_insert_query, record_to_insert)
    
        connection.commit()
        count = cursor.rowcount
        return ''
        #print(count, "Record inserted successfully into mobile table")
    
    except (Exception, psycopg2.Error) as error:
        return ("Failed to insert record into infraestructura", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
            
            
def reservas(lista,upid):
    
    upid_str = str(int(upid))
    lista1=lista[12:25]
    lista1.append(upid_str)
    a1=tuple(lista1)     
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_insert_query = """ INSERT INTO reservas (oxygen_reserves,antipyretics_reserves,
        anesthetics_and_muscular_relaxants,alcohol_reserves_and_handsoap,personal_disposable_masks,
        personal_vinyl_gloves,personal_disposable_hats,personal_disposable_aprons,
        personal_visors,personal_disposable_shoe_covers,test_kits,
        num_test_kits,respiratory_ventilator_machines,update_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CAST(%s AS NUMERIC),CAST(%s AS NUMERIC),CAST(%s AS NUMERIC))"""
        
        record_to_insert = a1
        cursor.execute(postgres_insert_query, record_to_insert)
    
        connection.commit()
        count = cursor.rowcount
        return ''
        #print(count, "Record inserted successfully into mobile table")
    
    except (Exception, psycopg2.Error) as error:
        return ("Failed to insert record into reservas", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
            

def casoscovid(lista,upid):
    upid_str = str(int(upid))
    lista1=lista[26:32]
    lista1.append(upid_str)
    a1=tuple(lista1)
    
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_insert_query = """ INSERT INTO casos_covid (positive_tests_last_month,
        intensive_care,phc_reffered_cases,deaths_last_month,non_covid_deaths,recovered_patients,update_id) values (CAST( %s AS NUMERIC),CAST( %s AS NUMERIC),CAST( %s AS NUMERIC),
                                                                                                CAST( %s AS NUMERIC),CAST( %s AS NUMERIC),CAST( %s AS NUMERIC),
                                                                                                CAST( %s AS NUMERIC))"""
        
        record_to_insert = a1
        cursor.execute(postgres_insert_query, record_to_insert)
    
        connection.commit()
        count = cursor.rowcount
        return ''
        #print(count, "Record inserted successfully into mobile table")
    
    except (Exception, psycopg2.Error) as error:
        return ("Failed to insert record into casos_covid", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
            
def seguimiento(lista,upid):
    upid_str = str(int(upid))
    lista1=lista[32:34]
    lista1.append(upid_str)
    a1=tuple(lista1)     
    
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_insert_query = """ INSERT INTO seguimiento (regular_tracking,moph_report_frecuency,update_id) values (%s,%s,CAST(%s AS NUMERIC))"""
        
        record_to_insert = a1
        cursor.execute(postgres_insert_query, record_to_insert)
    
        connection.commit()
        count = cursor.rowcount
        return ''
        #print(count, "Record inserted successfully into mobile table")
    
    except (Exception, psycopg2.Error) as error:
        return ("Failed to insert record into seguimiento", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")
            
def control(lista,upid):
    upid_str = str(int(upid))
    lista1=lista[34:36]
    lista1.append(upid_str)
    a1=tuple(lista1)    
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="admin",
                                      host="localhost",
                                      port="5432",
                                      database="proyectoFinal")
        cursor = connection.cursor()
    
        postgres_insert_query = """ INSERT INTO control_ (update_status,problem,update_id) values (%s,%s,CAST(%s AS NUMERIC))"""
        
        record_to_insert = a1
        cursor.execute(postgres_insert_query, record_to_insert)
    
        connection.commit()
        count = cursor.rowcount
        return ''
        #print(count, "Record inserted successfully into mobile table")
    
    except (Exception, psycopg2.Error) as error:
        return ("Failed to insert record into control_", error)
    
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print("PostgreSQL connection is closed")


# Función que lee todo el rango range_to_read y devuelve una lista de listas con los valores leídos
def read_all(credentials_, sheet_id, range_to_read): 
    
    service = build('sheets', 'v4', credentials=credentials_) 
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=range_to_read).execute()
    
    values = result.get('values', []) #regresa los valores en una lista de listas
    return values

def write_cell(credentials_,spreadsheet_id,j):
    service = build('sheets', 'v4', credentials=credentials_)
    sheet = service.spreadsheets()
    request = sheet.values().update(spreadsheetId=spreadsheet_id, range="Hoja 2!A1", valueInputOption="USER_ENTERED", body={"values":j})
    request.execute()

# Función que borra todos los datos de la spreadsheet 
def clear(credentials_, spreadsheet_id, range_):
    
    service = build('sheets', 'v4', credentials = credentials_)
    # Call the Sheets API
    sheet = service.spreadsheets()
    request = sheet.values().clear(spreadsheetId=spreadsheet_id, range=range_, body={}).execute()
    return request


def main():
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    # Credenciales de Google Cloud
    SERVICE_ACCOUNT_FILE = "C:\\Users\\javi2\\Documents\\Proyecto Bases de Datos\\keys.json" 
    creds = None
    creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    
    # ID de la spreadsheet que vamos a leer/escribir
    SPREADSHEET_ID = '1NrnirdVqhgThFQqfcyC8YquR7ciKqK_j6qingCOUF14'
    
    # Rango a leer/escribir en notación A1
    #sheet_range="Respuestas de formulario 1!A2:AJ" 
    sheet_range="Hoja 2!A2:AJ"
    
    respuestas = read_all(creds, SPREADSHEET_ID, sheet_range)
    j=read_all(creds,SPREADSHEET_ID,"Hoja 2!A1")
    
    try:
        
        j=int(j[0][0])
        
    except:
        j=0
    
    if(respuestas == []):
        print("No hay datos para actualizar")
    else:
        # Llenado de tablas
        k=j
        sheet_range = "Hoja 2!A"+str(2+k)+":AJ"+str(2+k)
        respuesta = read_all(creds, SPREADSHEET_ID, sheet_range)
        res = ''
        while respuesta != []:
            
            for i in range(0,len(respuesta[0])):
                if respuesta[0][i]=='':
                    respuesta[0][i]="-2"
                    
            sb = ''
            
            up_date = update(respuesta[0])
            if(up_date == 'Error'):
                sb = sb + str(up_date)
            else:
                upid = get_upid(up_date)
                sb = sb + str(infraestructura(respuesta[0],upid))
                sb = sb + str(reservas(respuesta[0], upid))
                sb = sb + str(casoscovid(respuesta[0], upid))
                sb = sb + str(control(respuesta[0], upid))
                sb = sb + str(seguimiento(respuesta[0], upid))
            if(sb!=''):
                res = "Error al actualizar. Por favor, revisar los registros del formulario"
            else:
                clear(creds, SPREADSHEET_ID, sheet_range)
            
            k=k+1
            sheet_range = "Hoja 2!A"+str(2+k)+":AJ"+str(2+k)
            respuesta = read_all(creds, SPREADSHEET_ID, sheet_range)
            l=[[k]]
            write_cell(creds, SPREADSHEET_ID,l)
            
        if res != '':
            print(res)
        else:
            print("La base de datos fue actualizada correctamente")
                    

main()