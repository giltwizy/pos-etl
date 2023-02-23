reporting_date = '2023-01-01' #----------------------[ENTER REPORTING DATE HERE]

import os, re
import numpy as np
import pandas as pd

def get_BusinessDay_range(reporting_date: str) -> None:
    try:
        init_dt, init_bzday = pd.Timestamp('2020-01-01'), 4190
        rdate = pd.Timestamp(reporting_date).replace(day=1)
        eomonth = (rdate + pd.DateOffset(months=1)) - pd.DateOffset(days=1)
        initial_bd = (rdate - init_dt).days + init_bzday
        fnal_day = initial_bd + (eomonth.day-1)
        print('For {}: Extract POS reports from {} to {}'.format(rdate.month_name(),initial_bd, fnal_day))
    except ValueError: print('Invalid')
get_BusinessDay_range(reporting_date)
pos_raw = 'Data Input'

def extract_totalReport(src:str) -> pd.DataFrame:
    '''
    Desc: Extract data from totalReport flat file and return pandas DataFrame
        src: source to totalReport flat file
    '''
    # Progress tracker setup
    line_counter, lines = 1,0
    with open(src,  'r') as file : lines = len([line for line in file])
    header_rw = ['^Merchant id:','^Business day:','^Totals number:','^Currency Code:','^Merchant Type:','^Store Number:']
    skip_rw = ['CRDB Bank', 'TRAN CODE', 'POS Terminal\'s', 'ISO 8583', '-------', '^$', 'SBE ', '===']
    data = list()

    with open(src) as input_file:
        buffer = []
        
        for row in input_file:
            # progress tracking
            line_counter += 1
            print('Extracting Total Report: {:.2f}%'.format(line_counter/lines*100), end='\r')
            
            # Skip unwanted lines and read other
            if re.search(r'|'.join(skip_rw), row): continue
            else: 
                # Extract the Merchants's info form the header part
                if re.search(r'|'.join(header_rw),row):
                    if re.search(r'Merchant id:',row): buffer = []
                    buffer.append(row)
                else:
                    header = ' '.join(buffer)
                    stl_tm = re.search(r'Time:(.*?)\n', header).group(1).strip()
                    stl_dt = re.search(r'Date:(.*?)Time', header).group(1).strip()
                    mer_id = re.search(r'Merchant id: ([0-9]+)', header).group(1).strip()
                    ter_id = re.search(r'Terminal ID:(.*?)Acquirer', header).group(1).strip()
                    biz_dy = re.search(r'Business day:(.*?)Date', header).group(1).strip()
                    ttl_nu = re.search(r'Totals number:(.*?)Merchant', header).group(1).strip()
                    mer_nm = re.search(r'Merchant Name:(.*?)\n', header).group(1).strip()
                    cur_cd = re.search(r'Currency Code:(.*?)Batch', header).group(1).strip()
                    bat_nu = re.search(r'Batch Number:(.*?)Host', header).group(1).strip()
                    hos_up = re.search(r'Host Upload file Number(.*?)\n', header).group(1).strip()
                    mer_ty = re.search(r'Merchant Type:(.*?)\n', header).group(1).strip()

                    data_row = pd.Series({'MerchantID':mer_id, 'TerminalID': ter_id, 'BusinessDay': biz_dy, 'Sattled_DT': stl_dt,  
                                          'Sattled_TM': stl_tm, 'TotalNumber': ttl_nu, 'MerchantName': mer_nm,'CurrencyCode': cur_cd, 'BatchNumber': bat_nu, 
                                          'HostUpFileNum': hos_up, 'MerchantType': mer_ty,'trx':row})
                    data.append(data_row)

    df = pd.DataFrame(data)
    df = df[df.trx.str.len() > 5].copy()

    # Converting trx string column into multiple columns
    mp =  map(df.trx.str.slice, [0, 10, 19, 28, 48, 61, 73, 92, 101, 108, 113, 118, 131, 146],
                                [10, 19, 28, 48, 61, 73, 92, 101, 108, 113, 118, 131, 146, 160])
    df['TranCode'], df['Terminal'], df['Date'], df['CardNumber'], df['EntryMode'], df['IDCode'], df['Amount'], df['Void AuthSRC'], df['APRVL'], df['Seq'], df['Time'],  df['CashBack'], df['TermFee'], df['MerchCOMM'] = mp
    df.drop(columns='trx', inplace=True)

    # Formant Date
    df.Sattled_DT = df.Sattled_DT.apply(lambda x: pd.Timestamp('20{}-{}-{}'.format(x[6:], x[3:5], x[:2])))
    df.Sattled_DT = df.Sattled_DT + pd.to_timedelta(((df.Sattled_TM.str[:2]).astype(int) + df.Sattled_TM.str[2:].astype(int)/60), unit='h')
    df['Settled'] = df.Sattled_DT.dt.date.astype('M')

    df.Date = df.Date.apply(lambda x: pd.Timestamp('20{}-{}-{}'.format(x[6:], x[3:5], x[:2])))
    df.Date = df.Date + pd.to_timedelta(((df.Time.str[:2]).astype(int) + df.Time.str[2:].astype(int)/60), unit='h')
    df['TRXDate'] = df.Date.dt.date.astype('M')
    df.drop(columns=['Sattled_TM', 'Time'], inplace=True)

    df.MerchCOMM = df.MerchCOMM.astype('float64') 
    df.Amount = df.Amount.apply(lambda x: x.replace('void', '')).astype('float64')
    return df

# ------------------------------------ [Run ETL]------------------------------------
for file in os.listdir(pos_raw):
    url = os.path.join(pos_raw, file)
    # Dispatch to the appropriate extract function
    if file.startswith('BAT'):
        ttl_report = extract_totalReport(url)
        ttl_report['ppn_month'] = file
        ttl_report.to_csv('Data Output\{}.csv'.format(file.strip('.txt')), index=False)
    print ('{} : Done'.format(file))