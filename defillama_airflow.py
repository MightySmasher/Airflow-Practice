import json
import datetime
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# Configuration
class Config:
    url = 'https://api.llama.fi/overview/fees/'
    lake = './data/'
    file_name = 'fees.csv'
    

# Will be use in get_historical_data() and get_daily_data()
def get_available_chain():
    response = requests.get(Config.url).text
    chains = json.loads(response)
    ls_chain = chains['allChains']
    print(type(ls_chain),'# of available chains:',len(ls_chain))
    df_chain = pd.DataFrame({"chain":ls_chain})
    return df_chain.to_csv(f"{Config.lake}/available_chain.csv",index=False,encoding='utf-8')

# Will be use in get_data()
def get_historical_data():
    print('listing available chains')
    df_chain = pd.read_csv(f"{Config.lake}/available_chain.csv")
    ls_chain = df_chain['chain']
    
    print("retriving historical data..")
    df_fees = pd.DataFrame()
    for i in ls_chain:
        # Extract
        try:
            destination = requests.get(Config.url+i).json()
        except:
            pass
        
        # Transform 
        try:
            protocols = pd.DataFrame(destination['protocols'])
            protocols_info = protocols[['defillamaId','displayName','module','category','protocolType']]

            totalDataChartBreakdown = pd.DataFrame(destination['totalDataChartBreakdown'],columns=['timestamp','dict'])            
            normalized = pd.json_normalize(totalDataChartBreakdown['dict'])
            normalized['timestamp'] = totalDataChartBreakdown['timestamp'].apply(lambda x : datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d'))
            transformed = pd.melt(normalized, id_vars=['timestamp'], value_vars=normalized.columns, var_name='displayName', value_name='fees')
            
            transformed['chain'] = i
            merged = transformed.merge(protocols_info,how='left', left_on='displayName', right_on='displayName').sort_values(by='timestamp')
            df_fees = pd.concat([df_fees,merged])
            df_fees = df_fees[['timestamp','defillamaId','displayName','module','category','protocolType','chain','fees']]
        except:
            pass
    return df_fees

# Will be use in get_data()
def get_daily_data():
    print('listing available chains')
    df_chain = pd.read_csv(f"{Config.lake}/available_chain.csv")
    ls_chain = df_chain['chain']

    print('retriving data..')
    new_batch = pd.DataFrame()
    for i in ls_chain:
        # Extract
        try:
            response = requests.get(str(Config.url+i)).json()
        except:
            pass
        
        # Transform
        try:
            new_records = pd.DataFrame(response['protocols'])
            new_records['timestamp'] = (datetime.datetime.today().astimezone(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            new_records['chain'] = i
            new_records.rename(columns={'dailyFees':'fees'}, inplace=True)
            new_records = new_records[['timestamp','defillamaId','displayName','module','category','protocolType','chain','fees']]
            new_batch = pd.concat([new_batch,new_records])
        except:
            pass
    return new_batch

def get_data():
    # Check if file is already in your datalake or not
    try:
        df = f"{Config.lake}/{Config.file}"
        
    # If not. get historical data from defillama api
    except:
        print("file is missing.. start getting historical data")
        create_new_file = get_historical_data()
        print('completed!!')
        return create_new_file
    
    # If yes. Check if the data is up to date
    try:
        current_date = datetime.datetime.today().astimezone(datetime.timezone.utc).strftime('%Y-%m-%d')
        last_updated = max(df.timestamp)
        
        date_diff = pd.to_datetime(current_date) - pd.to_datetime(last_updated)
        n_diff = date_diff.days

        print(f"current_date={current_date}, last_transaction={last_updated}, timedelta={n_diff} days")
    
    # If up to date
        if n_diff <= 1:
            print('file has already up to date!! please waiting for DefiLlama to update at 00.00UTC')
            pass
    
    # If not up to date. Daily update
        elif n_diff == 2:
            
            print('updating...daily data')
            daily_data = get_daily_data()
            daily_updated = pd.concat([df,daily_data])

            print('complete!!')
            return daily_updated     

    # If not up to date with missing update    
        elif n_diff > 2:
            print(f'missing more than {n_diff} days')
            his_data = get_historical_data()
            missing_data = his_data[his_data['timestamp'] > last_updated]

            if len(missing_data) == 0:
                pass
            else:
                missing_updated = pd.concat([df,missing_data])
                print('complete!!')
                return missing_updated
    except:
        pass

def load_to_lake():
    api_data = get_data()
    api_data.to_csv(f"{Config.lake}/{Config.file_name}", encoding='utf-8', index=False)


# Default Args
default_args = {
    'owner': 'User',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Create DAG
with DAG('Defillama',
    default_args=default_args,
    description='Pipeline for ETL Defillama',
    schedule_interval= '0 12 * * *'
) as dag:

    t1 = PythonOperator(
        task_id='get_available_chain',
        python_callable=get_available_chain,
        dag=dag,
    )

    t2 = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id='load_to_lake',
        python_callable=load_to_lake,
        dag=dag,
    )

    t1 >> t2 >> t3