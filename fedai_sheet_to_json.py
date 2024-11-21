import pandas as pd
from datetime import datetime
import calendar
import json
import requests
from try_api import getFwdRate


# Function to get the end date of a given month
def get_month_end_date(year_month):
    year, month = map(int, year_month.split('-'))
    last_day = calendar.monthrange(year, month)[1]
    return datetime(year, month, last_day).strftime('%Y-%m-%d')

# Encryption function
def encrypt(pt, key=15):
    try:
        pt = float(pt)
        formatNumber = lambda n: n if n % 1 else int(n)
        new_string = str(formatNumber(pt))[::-1]
        array = [char for char in new_string]
        enc_text = ""
        for char in array:
            temp = str(int(ord(char)) + int(key))
            enc_text = enc_text + temp[::-1]
        return enc_text
    except ValueError:
        return pt

# Function to read the Excel file and process data
def read_excel(filename, tab_name, bottom_rows):
    df = pd.read_excel(filename, sheet_name=tab_name, skiprows=11)
    df = df.drop(df.index[0])
    df = df.iloc[:-bottom_rows]
    df = df.fillna('')

    # Calculate premium values for each month
    code = df['CODE']
    spot = df['SPOT']
    premiums = []

    # Calculating premiums for each month
    for i in range(2, 12):
        premiums.append((df.iloc[:, i].apply(pd.to_numeric, errors='coerce') - df['SPOT']).round(4))

    # Create a new DataFrame for the premium values
    premium_df = pd.DataFrame({
        'code': code,
        'spot': spot,
    })

    for i, premium in enumerate(premiums):
        premium_df[df.columns[i + 2]] = premium

    # Concatenate the premium DataFrame with the original DataFrame
    result_df = pd.concat([df[['CODE', 'SPOT']], premium_df.drop(columns=['code', 'spot'])], axis=1)

    # Renaming the columns
    new_columns = {}
    for col in result_df.columns:
        try:
            new_columns[col] = datetime.strptime(col, "%b. %Y").strftime("%Y-%m")
        except ValueError:
            new_columns[col] = col

    result_df.rename(columns=new_columns, inplace=True)
    result_df.fillna(0, inplace=True)

    # # Create a mapping from existing column names to month-end dates
    # date_mapping = {}
    # for date_str in new_columns.values():
    #     if '-' in date_str:
    #         month_end_date = get_month_end_date(date_str)
    #         date_mapping[date_str] = month_end_date

    # # Update the column names in result_df using the date_mapping
    # renamed_columns = {k: v for k, v in date_mapping.items() if k in result_df.columns}
    # result_df.rename(columns=renamed_columns, inplace=True)
    url='https://cms.greenbackforex.net/media/month_end_dates_json/month_end_dates_history.json'

    response = requests.get(url, verify=False)

    json_month_end_dates=response.json()

    month_end_dates = json_month_end_dates['month_end_dates']

    headings = result_df.columns.tolist()

    Iwantdate = ['code','spot']

    for i in headings:
        for j in month_end_dates:
            if i in j :
                print(j)
                Iwantdate.append(j)

    result_df.columns = Iwantdate


    headings = result_df.columns.tolist()

    # Prompt user for Spot date input
    sp_date = input("Enter Spot date: ")
    new_column = headings[2:]
    new_column.insert(0, sp_date)

    # Transpose the DataFrame
    df_transposed = result_df.T

    # Rename the columns of the transposed DataFrame
    df_transposed.columns = df_transposed.iloc[0]
    df_transposed = df_transposed[1:].reset_index(drop=True)

    # Insert the Date column
    df_transposed.insert(0, 'Date', new_column)

    usdask = df_transposed['USD'].to_list()
    df_transposed['USD ASK'] = usdask

    # Save the DataFrame to an Excel file
    output_filename = 'Fedai_Rates_with_Premiums.xlsx'
    df_transposed.to_excel(output_filename, index=False)

    return df_transposed

filename = 'Revaluation_Rates_28_03_2024.xlsx'
tab_name = 'Sheet1'
bottom_rows_to_skip = 6    
df_transposed = read_excel(filename, tab_name, bottom_rows_to_skip)


# print(df_transposed)

usdbidlist = df_transposed['USD'].to_list()
usdasklist = df_transposed['USD ASK'].to_list()
usddatelist = df_transposed['Date'].to_list()

# print(usdbidlist)





def readLTFX(filename, sheet):
    df = pd.read_excel(filename, sheet)
    return df

ltfx = readLTFX(filename, 'LTFX')


ltfx['Date'] = ltfx['Date'].astype(str)

ltfx['Date'] = pd.to_datetime(ltfx['Date']).dt.strftime('%Y-%m-%d')

ltfxdate= ltfx['Date'].to_list()
ltfxBid = ltfx['Bid'].to_list()
ltfxAsk = ltfx['Ask'].to_list()


usdbidlist.extend(ltfxBid)
usdasklist.extend(ltfxAsk)
usddatelist.extend(ltfxdate)



usd_symbol = ['USDINRCOMP', 'USDINRCOMP_Fwd_ME_0M', 'USDINRCOMP_Fwd_ME_1M', 'USDINRCOMP_Fwd_ME_2M',
              'USDINRCOMP_Fwd_ME_3M', 'USDINRCOMP_Fwd_ME_4M', 'USDINRCOMP_Fwd_ME_5M', 'USDINRCOMP_Fwd_ME_6M',
              'USDINRCOMP_Fwd_ME_7M', 'USDINRCOMP_Fwd_ME_8M', 'USDINRCOMP_Fwd_ME_9M', 'USDINRCOMP_Fwd_ME_10M', 
              'USDINRCOMP_Fwd_ME_11M', 'USDINRCOMP_Fwd_ME_1Y', 'USDINRCOMP_Fwd_ME_2Y', 'USDINRCOMP_Fwd_ME_3Y', 
              'USDINRCOMP_Fwd_ME_4Y', 'USDINRCOMP_Fwd_ME_5Y', 'USDINRCOMP_Fwd_ME_6Y', 'USDINRCOMP_Fwd_ME_7Y', 
              'USDINRCOMP_Fwd_ME_8Y', 'USDINRCOMP_Fwd_ME_9Y']

finalDf = pd.DataFrame({
    'Date' : usddatelist,
    'USD Bid' : usdbidlist,
    'USD Ask' : usdasklist,
    'Curr Symbol' : usd_symbol
})

# print(finalDf)
# data = {
#     "month_end_dates_usd": ','.join(finalDf['Date'].tolist()[1:])
# }
data = {}

def add_data_to_dict(df, data):
    def update_data(row):

        data['Spot_Date'] = df.iloc[0]['Date']
        data['SUSDINR_ASK'] = encrypt(df.iloc[0]['USD Bid'])
        data['SUSDINR_BID'] = encrypt(df.iloc[0]['USD Ask'])

        data["month_end_dates_usd"] = ','.join(df['Date'].tolist()[1:])
        currency_symbol = row['Curr Symbol']
        data[f"{currency_symbol}_BID"] = encrypt(row["USD Bid"])
        data[f"{currency_symbol}_ASK"] = encrypt(row["USD Ask"])

        spot_rate_symbol = df['Curr Symbol'][0]

        data[f"{spot_rate_symbol}_BID"] = encrypt(df.iloc[0]['USD Bid'])
        data[f"{spot_rate_symbol}_ASK"] = encrypt(df.iloc[0]['USD Ask'])

        
    df.apply(update_data, axis=1)
add_data_to_dict(finalDf, data)


json_str = json.dumps(data, indent=4)

# Write the JSON string to a file
output_file = "quarter_end_data.json"
with open(output_file, 'w') as f:
    f.write(json_str)


