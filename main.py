from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
import time
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service
import random
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import pandas as pd
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import pickle
import os




def gsheet_api_check(SCOPES):
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def pull_sheet_data(SCOPES,SPREADSHEET_ID,DATA_TO_PULL):
    creds = gsheet_api_check(SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=DATA_TO_PULL).execute()
    values = result.get('values', [])
    
    if not values:
        print('No data found.')
    else:
        rows = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=DATA_TO_PULL).execute()
        data = rows.get('values')
        print("COMPLETE: Data copied")
        return data




# This is for setting firefox for Headless
firefox_options = Options()
firefox_options.headless = True

# Here Install GeckoDriver and start Firefox in headless 
driver = webdriver.Firefox(service=Service(executable_path=GeckoDriverManager().install()), options=firefox_options)

a=random.randint(30,90)

# Navigate to the website
driver.get("https://www.rbi.org.in/scripts/WSSViewDetail.aspx?PARAM1=2&TYPE=Section")


# Wait for the table to load
wait = WebDriverWait(driver, a)
table = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="annual"]/table[2]/tbody/tr[2]/td[1]/a')))
table.click()
time.sleep(20)


html = driver.page_source
soup = BeautifulSoup(html, "html.parser")



# In[11]:


commonclass = soup.find_all('td')
for i in range (len(commonclass)):
    if "Total Reserves" in commonclass[i].text:
        reserves =  (commonclass[i+2].text)
        # print(reserves)
    
        change = (commonclass[i+4].text)
        print(change)


# print(commonclass)
# In[12]:


# print(reserves, change)


# In[ ]:

driver.close()

# picking up the update Date 
Date = (commonclass[4].text[6:])

Date = Date.replace('.', '')

# print("Date----------------------------------------------->",Date)

#converting to datetime format
# try:
#     Date1 = datetime.strptime(Date, '%B %d, %Y')
# except:
#     Date1 = datetime.strptime(Date, '%B %d %Y')
    


try:
    Date1 = datetime.strptime(Date, '%B %d, %Y')  # Using abbreviated month format without a period
except ValueError as e:
    print("Error parsing date:", e)

# creating a dict to create write a df and saving to csv
Reserves ={
    'RBI Date': Date1,
    'RBI Reserve': float(reserves)/1000
    # 'Change': change
}
dfReserves = pd.DataFrame(Reserves, index=[0])

print(dfReserves)
# dfReserves.to_csv('reserves.csv', index=False)

# dfReservesOld = pd.read_csv('OnlineData - RBI.csv')

## read from Google sheet

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1L1Pyu-G40w7sblVOB-nyIHN5vUMFb5LA-IeolPgM6jM'
DATA_TO_PULL = 'RBI'
data = pull_sheet_data(SCOPES,SPREADSHEET_ID,DATA_TO_PULL)
dfReservesOld = pd.DataFrame(data[1:], columns=data[0])
# print(dfReservesOld)

# dfReserves['RBI Date'] = pd.to_datetime(dfReserves['RBI Date'],format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
# dfReservesOld['RBI Date'] = pd.to_datetime(dfReservesOld['RBI Date'],format='%d-%b-%Y').dt.strftime('%Y-%m-%d')

dfReserves['RBI Date'] = dfReserves['RBI Date'].dt.strftime('%d-%b-%Y')

# dfReservesOld = pd.read_csv('onlineData - RBI.csv')
# dfReserves = pd.read_csv('reserves.csv')
newDate = dfReserves['RBI Date'][-1:]
newDate = list(pd.to_datetime(newDate).dt.date)[0]
# print(newDate)

oldDate = dfReservesOld['RBI Date'][-1:]
oldDate = list(pd.to_datetime(oldDate).dt.date)[0]

if newDate != oldDate:
    dfReservesNew = pd.concat([dfReservesOld,dfReserves])
    dfReservesNew.reset_index(inplace=True,drop=True)
else:
    dfReservesNew = dfReservesOld

rows = len(dfReservesNew)

## Update data to Google sheets

# # code to read / write / delete to google sheets to / from a data frame
    
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# here enter the id of your google sheet
SAMPLE_SPREADSHEET_ID_input = '1L1Pyu-G40w7sblVOB-nyIHN5vUMFb5LA-IeolPgM6jM'

# code to create service credentials
gsheetId = SAMPLE_SPREADSHEET_ID_input
#change the range if needed
# SAMPLE_RANGE_NAME = 'A!A1:AA1000'

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    #print(SCOPES)
    
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        # print(api_service_name, 'service created successfully')
        #return service
    except Exception as e:
        print(e)
        #return None
    return

# change 'my_json_file.json' by your downloaded JSON file.
Create_Service('credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])

def clear_google_sheet(gsheetId, clearRange):
    ### function to clear google sheet based on sheet id and range of cells to be cleared ###
    range_ = clearRange
    credentials = None
    spreadsheet_id = gsheetId
    clear_values_request_body = {
    # TODO: Add desired entries to the request body.
    }

    request = service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=range_, body=clear_values_request_body)
    response = request.execute()

    # TODO: Change code below to process the `response` dict:
    # pprint(response)
    return

# Clear the google sheet
clearRange = str('RBI!A1:D'+str(rows+10))
print(clearRange)
clear_google_sheet(gsheetId, clearRange)

def Export_Data_To_Sheets(sheetName, df):
#     SAMPLE_RANGE_NAME = create_range_upload(sheetName,df,alphabet_list)
    SAMPLE_RANGE_NAME = clearRange
    response_date = service.spreadsheets().values().update(
        spreadsheetId=gsheetId,
        valueInputOption='USER_ENTERED',
        range=SAMPLE_RANGE_NAME,
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()
    # print('Sheet successfully Updated')
    return
# DfNew = pd.read_csv('FF_data.csv')
Export_Data_To_Sheets('RBI',dfReservesNew)

dfReservesNew.to_csv('OnlineData - RBI.csv', index=False)

# send message to Telegram Group
print(newDate)
if newDate != oldDate:
    message = "GitHub Actions: RBI code run successfully"
else:
    message  = "GitHub Actions: RBI Data already Updated!"

def telegram_bot_sendtext(bot_message):
    ### function to send message by bot to Group"
    bot_token = '1436434204:AAEe2TmCTBF1p8jmszXwo15pWSRD7bURJQM'
    bot_chatID = '-1001168072098'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
# calling the function to send message
response = telegram_bot_sendtext(message)
print("test")
