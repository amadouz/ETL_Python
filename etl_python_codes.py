# Comments on how to extract data from XML file 
# To extract from an XML file, you need first to parse the data from the file using the ElementTree function. You can then extract relevant information from this data and append it to a pandas dataframe as follows.
# Note: You must know the headers of the extracted data to write this function. In this data, you extract "name", "height", and "weight" headers for different persons.


import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index = True)
    return dataframe

def extract():
    # Create empty dataFrame to hold extracted data
    extracted_data = pd.DataFrame(columns = ["name", "height", "weight"])

    # Process all CSV files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)
    
    # Process all Json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    # Process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True)

    
    return extracted_data

def transform(data):

    ''' Convert inches to meters and round off to two decimals 1 inch is 0.0254 meters'''
    data['height'] = round(data.height * 0.0254,2)

    ''' Convert pounds to kilograms and round off to two decimals 1 pound is 0.45359237 Kilograms'''
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_process(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Testing ETL operations and log progress

# Log the initialisation of the ETL process
log_process("ETL Job Started")

# Log the beginning of the Extraction process
extracted_data = extract()

# Log the completion of the Extraction process
log_process("Extract phase Ended")

# Log the beginning of the Transformation process
log_process("Transformation phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_process("Transformation phase Ended")

# Log the beginning of the Loading process
log_process("Load phase started")
load_data(target_file,transformed_data)

# Log the completion of the Loading process
log_process("Load phase Ended")

# Log the completion of the ETL process
log_process("ETL Job Ended")






wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip







2024-Jun-02-17:32:43,ETL Job Started
2024-Jun-02-17:32:43,Extract phase Ended
2024-Jun-02-17:33:18,ETL Job Started
2024-Jun-02-17:33:18,Extract phase Ended
2024-Jun-02-17:33:56,ETL Job Started
2024-Jun-02-17:33:56,Extract phase Ended
2024-Jun-02-17:33:56,Transformation phase Started
2024-Jun-02-17:34:44,ETL Job Started
2024-Jun-02-17:34:44,Extract phase Ended
2024-Jun-02-17:34:44,Transformation phase Started
2024-Jun-02-17:34:44,Transformation phase Ended
2024-Jun-02-17:34:44,Load phase started
2024-Jun-02-17:34:44,Load phase Ended
2024-Jun-02-17:34:44,ETL Job Ended
2024-Jun-02-17:36:36,ETL Job Started
2024-Jun-02-17:37:03,ETL Job Started
2024-Jun-02-17:37:27,ETL Job Started
2024-Jun-02-17:37:51,ETL Job Started
2024-Jun-02-17:38:31,ETL Job Started
2024-Jun-02-17:38:31,Extract phase Ended
2024-Jun-02-17:38:31,Transformation phase Started
2024-Jun-02-17:38:31,Transformation phase Ended
2024-Jun-02-17:38:31,Load phase started
2024-Jun-02-17:38:31,Load phase Ended
2024-Jun-02-17:38:31,ETL Job Ended









# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd 
import sqlite3 
import numpy as np 
from datetime import datetime

URL = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name','MC_USD_Billion']
table_attributes_final = ['Name','MC_USD_BILLION','MC_GBP_BILLION','MC_EUR_BILLION','MC_INR_BILLION']
db_name = 'banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
csv_file = 'exchange_rate.csv'
log_file = 'code_log.txt'
sql_connection = sqlite3.connect('Banks.db')

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp +' : ' + message+ '\n')

# Log the initialisation of the ETL process
log_progress("Preliminaries complete. Initiating ETL process")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df = pd.DataFrame(columns=table_attribs)
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    rows = soup.find_all('table')[0].find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) != 0:
           data_dict = {
            table_attribs[0] : cells[1].text.strip(),
            table_attribs[1] : float(cells[2].text.strip())
           }
           df = pd.concat([df,pd.DataFrame(data_dict, index=[0])], ignore_index = True)

    return df

extracted_data = extract(URL,table_attribs)
# Log the completion of data extraction
log_progress("Data extraction complete. Initiating Transformation process")

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate = pd.DataFrame(data = pd.read_csv(csv_path),columns = ['Currency','Rate'])
    exchange_rate_dict = exchange_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate_dict['EUR'],2) for x in df['MC_USD_Billion']]  
    df['MC_INR_Billion'] = [np.round(x*exchange_rate_dict['INR'],2) for x in df['MC_USD_Billion']]  

    return df

transformed_data = transform(extracted_data,csv_file)
# Log the end of Data transformation
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


load_to_csv(transformed_data,csv_path)
# Log the data saving in the CSV file 
log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

load_to_db(transformed_data,sql_connection,table_name)
# Log the DB connection
log_progress("SQL Connection initiated")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    return pd.read_sql(query_statement,sql_connection)


#run_query(query_statement, sql_connection)
print(run_query("SELECT * FROM Largest_banks", sql_connection))

print(run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection))

print(run_query("SELECT Name from Largest_banks LIMIT 5", sql_connection))

# Log the completion of SQL execution 
log_progress("Process Complete")


# Log Closing of DB Connection
log_progress("Server Connection closed")

# End of program






