import json

def lambda_handler(event, context):
    import requests
    from bs4 import BeautifulSoup as bs
    import json
    import pandas as pd
    import gspread_dataframe as gd 
    r = requests.get("https://queue-times.com/en-US/parks/16/queue_times.json")

    todos = json.loads(r.text)
    
    rides = []
    wait_times = []
    is_open = []
    last_update = []
    lands = []
    park = []


    for i in range(0,len(todos['lands'])):
        for j in range(0,len(todos['lands'][i]['rides'])):
            rides.append(todos['lands'][i]['rides'][j]['name'])
            wait_times.append(todos['lands'][i]['rides'][j]['wait_time'])
            is_open.append(todos['lands'][i]['rides'][j]['is_open'])
            last_update.append(todos['lands'][i]['rides'][j]['last_updated'])
            lands.append(todos['lands'][i]['name'])
            park.append('Disneyland')
            
    r1 = requests.get("https://queue-times.com/en-US/parks/17/queue_times.json")

    todos1 = json.loads(r1.text)



    for i in range(0,len(todos1['lands'])):
        for j in range(0,len(todos1['lands'][i]['rides'])):
            rides.append(todos1['lands'][i]['rides'][j]['name'])
            wait_times.append(todos1['lands'][i]['rides'][j]['wait_time'])
            is_open.append(todos1['lands'][i]['rides'][j]['is_open'])
            last_update.append(todos1['lands'][i]['rides'][j]['last_updated'])
            lands.append(todos1['lands'][i]['name'])
            park.append('California Adventure')

    disney_dataset = pd.DataFrame(list(zip(park,lands,rides,wait_times,is_open,last_update)), columns = ['park','lands','rides','wait_times','is_open','last_update'] )
    
    import gspread
    #from df2gspread import df2gspread as d2g
    from oauth2client.client import GoogleCredentials
    from oauth2client.service_account import ServiceAccountCredentials
    
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'disney_project.json', scope)
    
    gc = gspread.authorize(credentials)

    spreadsheet_key = '1zEYaQs5xKoNa0ZKR4arErSexKfgcbVdZLA_gei6n7II'
    wks_name = 'Wait Times Disneyland and CA'
    #d2g.upload(disney_dataset, spreadsheet_key,wks_name, credentials = credentials, row_names = False)
    ws = gc.open('Disney Ride Times').worksheet(wks_name)
    existing = gd.get_as_dataframe(ws)
    existing.dropna(how='all', axis=1, inplace=True)
    existing.dropna(axis=0,
        how='any',
        thresh=None,
        subset=None,
        inplace=True)
    updated = existing.append(disney_dataset)
# d2g.upload(disney_dataset,spreadsheet_key,wks_name,credentials=credentials,row_names=False)
    gd.set_with_dataframe(ws,updated)
    
