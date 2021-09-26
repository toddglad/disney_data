import gspread
# from df2gspread import df2gspread as d2g
from oauth2client.client import GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd 
import csv
import datetime as dt
import pandas as pd


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    'disney_project.json', scope)


gc = gspread.authorize(credentials)

spreadsheet_key = '1zEYaQs5xKoNa0ZKR4arErSexKfgcbVdZLA_gei6n7II'
wks_name = 'Wait Times Disneyland and CA'
ws = gc.open('Disney Ride Times').worksheet(wks_name)
existing = gd.get_as_dataframe(ws)
existing.dropna(how='all', axis=1, inplace=True)
# print(existing)
existing.dropna(axis=0,
    how='any',
    thresh=None,
    subset=None,
    inplace=True)
# updated = existing.append(disney_dataset)
# d2g.upload(disney_dataset,spreadsheet_key,wks_name,credentials=credentials,row_names=False)
# gd.set_with_dataframe(ws,updated)
# headers = ['park','lands','rides','wait_times','is_open','last_update']

write_back = existing[existing['last_update'] == max(existing['last_update'])]
# existing['data_added'] = dt.datetime.now()
# existing.to_csv("disney_data.csv",index=False, mode = 'a', header = False)
# gd.set_with_dataframe(ws,writeback)
write_to_csv = existing[existing['last_update'] != max(existing['last_update'])]
write_back2 = write_to_csv[write_to_csv['last_update'] == max(write_to_csv['last_update'])]
write_back = write_back.append(write_back2)
ws.clear()
gd.set_with_dataframe(ws,write_back)
# write_to_csv = write_to_csv[write_to_csv['last_update']!= max(write_to_csv['last_update'])]
# write_to_csv.to_csv("disney_data.csv",index=False)
# print(write_to_csv)
write_to_csv.to_csv("disney_data.csv", mode = 'a', index = False, header = False)
